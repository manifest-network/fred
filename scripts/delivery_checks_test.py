"""Regression checks for coverage and release-source admission; no network needed."""

from decimal import Decimal
import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPTS = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("coverage_check", SCRIPTS / "check-coverage.py")
COVERAGE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COVERAGE)


class CoverageTests(unittest.TestCase):
    def test_floor_uses_statement_counts_including_uncovered_packages(self):
        with tempfile.TemporaryDirectory() as directory:
            profile = Path(directory) / "coverage.out"
            profile.write_text("mode: set\nexample/a/a.go:1.1,2.1 76 1\n"
                               "example/b/b.go:1.1,2.1 24 0\n")
            output, passed = COVERAGE.report(profile, Decimal("76.0"))
            self.assertTrue(passed)
            self.assertIn("76.00% (76/100)", output)
            self.assertIn("`example/b` | 0.00%", output)
            profile.write_text("mode: set\nexample/a/a.go:1.1,2.1 7599 1\n"
                               "example/b/b.go:1.1,2.1 2401 0\n")
            _, passed = COVERAGE.report(profile, Decimal("76.0"))
            self.assertFalse(passed, "rounding must not hide a regression")

    def test_empty_or_malformed_profiles_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            profile = Path(directory) / "coverage.out"
            for text in ("", "mode: set\n", "mode: set\ninvalid\n"):
                profile.write_text(text)
                with self.assertRaises(ValueError):
                    COVERAGE.report(profile, Decimal("76.0"))


class ReleaseSourceTests(unittest.TestCase):
    def test_unmerged_descendant_is_not_release_authority(self):
        with tempfile.TemporaryDirectory() as directory:
            def git(*arguments):
                return subprocess.run(["git", *arguments], cwd=directory, check=True,
                                      capture_output=True, text=True).stdout.strip()

            def allowed():
                return subprocess.run(["bash", str(SCRIPTS / "check-release-source.sh")],
                                      cwd=directory, capture_output=True).returncode == 0

            git("init", "-b", "main")
            git("-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                "commit", "--allow-empty", "-m", "reviewed")
            reviewed = git("rev-parse", "HEAD")
            git("update-ref", "refs/remotes/origin/main", reviewed)
            self.assertTrue(allowed())
            git("switch", "-c", "unmerged")
            git("-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                "commit", "--allow-empty", "-m", "not reviewed")
            self.assertFalse(allowed())
            git("update-ref", "refs/remotes/origin/main", git("rev-parse", "HEAD"))
            self.assertTrue(allowed())
            git("checkout", "--detach", reviewed)
            self.assertTrue(allowed(), "older merged releases remain possible")


if __name__ == "__main__":
    unittest.main()
