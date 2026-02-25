#!/usr/bin/env python3
"""Validates docs/manifest-schema.json against positive and negative test cases.

Uses the JSON Schema definition to approximate the Go validation logic
in internal/backend/docker/manifest.go, but does not cover every backend rule.

Requires the 'jsonschema' package (install with: pip install jsonschema).
Run: python3 docs/manifest-schema_test.py
"""

import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator, ValidationError

SCHEMA_PATH = Path(__file__).parent / "manifest-schema.json"


def load_schema():
    with open(SCHEMA_PATH) as f:
        schema = json.load(f)
    Draft202012Validator.check_schema(schema)
    return schema


def validate(schema, instance):
    """Returns None on success, or a ValidationError on failure."""
    validator = Draft202012Validator(schema)
    errors = list(validator.iter_errors(instance))
    return errors[0] if errors else None


def main():
    schema = load_schema()
    passed = 0
    failed = 0

    def expect_valid(name, instance):
        nonlocal passed, failed
        err = validate(schema, instance)
        if err is None:
            passed += 1
        else:
            failed += 1
            print(f"FAIL (expected valid): {name}")
            print(f"  Error: {err.message}")

    def expect_invalid(name, instance, keyword=None):
        nonlocal passed, failed
        err = validate(schema, instance)
        if err is not None:
            passed += 1
        else:
            failed += 1
            print(f"FAIL (expected invalid): {name}")

    # ---------------------------------------------------------------
    # Single-service manifests — valid
    # ---------------------------------------------------------------
    expect_valid("minimal single-service", {
        "image": "nginx:latest"
    })

    expect_valid("single-service with ports", {
        "image": "nginx:latest",
        "ports": {
            "80/tcp": {"host_port": 8080},
            "53/udp": {}
        }
    })

    expect_valid("single-service with env", {
        "image": "nginx:latest",
        "env": {"DATABASE_URL": "postgres://localhost/db", "APP_PORT": "3000"}
    })

    expect_valid("single-service with health check CMD", {
        "image": "nginx:latest",
        "health_check": {
            "test": ["CMD", "curl", "-f", "http://localhost/"],
            "interval": "10s",
            "timeout": "5s",
            "retries": 3,
            "start_period": "30s"
        }
    })

    expect_valid("single-service with health check CMD-SHELL", {
        "image": "nginx:latest",
        "health_check": {
            "test": ["CMD-SHELL", "curl -f http://localhost/ || exit 1"],
            "interval": "30s"
        }
    })

    expect_valid("single-service with health check NONE", {
        "image": "nginx:latest",
        "health_check": {"test": ["NONE"]}
    })

    expect_valid("single-service with tmpfs", {
        "image": "nginx:latest",
        "tmpfs": ["/var/cache/nginx", "/var/log/nginx"]
    })

    expect_valid("single-service with user uid", {
        "image": "postgres:16",
        "user": "999"
    })

    expect_valid("single-service with user uid:gid", {
        "image": "postgres:16",
        "user": "999:999"
    })

    expect_valid("single-service with user name:group", {
        "image": "postgres:16",
        "user": "postgres:postgres"
    })

    expect_valid("single-service with stop_grace_period", {
        "image": "nginx:latest",
        "stop_grace_period": "10s"
    })

    expect_valid("single-service with init true", {
        "image": "nginx:latest",
        "init": True
    })

    expect_valid("single-service with init false", {
        "image": "nginx:latest",
        "init": False
    })

    expect_valid("single-service with expose", {
        "image": "nginx:latest",
        "expose": ["3000", "8080"]
    })

    expect_valid("single-service with command and args", {
        "image": "nginx:latest",
        "command": ["/bin/sh", "-c"],
        "args": ["echo hello"]
    })

    expect_valid("single-service with labels", {
        "image": "nginx:latest",
        "labels": {"app": "myapp", "version": "1.0"}
    })

    expect_valid("single-service compound duration", {
        "image": "nginx:latest",
        "stop_grace_period": "1m30s"
    })

    expect_valid("single-service nanosecond duration", {
        "image": "nginx:latest",
        "health_check": {
            "test": ["CMD", "true"],
            "interval": 30000000000
        }
    })

    expect_valid("single-service all fields", {
        "image": "nginx:latest",
        "ports": {"80/tcp": {"host_port": 8080}},
        "env": {"APP": "test"},
        "command": ["/entrypoint.sh"],
        "args": ["--debug"],
        "labels": {"env": "prod"},
        "health_check": {
            "test": ["CMD-SHELL", "curl localhost"],
            "interval": "10s",
            "timeout": "3s",
            "retries": 5,
            "start_period": "1m"
        },
        "tmpfs": ["/var/cache"],
        "user": "1000:1000",
        "stop_grace_period": "30s",
        "init": True,
        "expose": ["9090"]
    })

    # ---------------------------------------------------------------
    # Single-service manifests — invalid
    # ---------------------------------------------------------------
    expect_invalid("empty object (missing image)", {})

    expect_invalid("empty image string", {"image": ""})

    expect_invalid("unknown field rejected", {
        "image": "nginx", "volumes": ["/data"]
    })

    expect_invalid("depends_on forbidden in single-service", {
        "image": "nginx",
        "depends_on": {"db": {"condition": "service_started"}}
    })

    expect_invalid("port missing protocol", {
        "image": "nginx",
        "ports": {"80": {}}
    })

    expect_invalid("port zero", {
        "image": "nginx",
        "ports": {"0/tcp": {}}
    })

    expect_invalid("port exceeds 65535", {
        "image": "nginx",
        "ports": {"70000/tcp": {}}
    })

    expect_invalid("port invalid protocol", {
        "image": "nginx",
        "ports": {"80/http": {}}
    })

    expect_invalid("host_port exceeds 65535", {
        "image": "nginx",
        "ports": {"80/tcp": {"host_port": 70000}}
    })

    expect_invalid("host_port negative", {
        "image": "nginx",
        "ports": {"80/tcp": {"host_port": -1}}
    })

    expect_invalid("env blocked PATH", {
        "image": "nginx",
        "env": {"PATH": "/usr/bin"}
    })

    expect_invalid("env blocked path (lowercase)", {
        "image": "nginx",
        "env": {"path": "/usr/bin"}
    })

    expect_invalid("env blocked Path (mixed case)", {
        "image": "nginx",
        "env": {"Path": "/usr/bin"}
    })

    expect_invalid("env blocked LD_PRELOAD", {
        "image": "nginx",
        "env": {"LD_PRELOAD": "/tmp/evil.so"}
    })

    expect_invalid("env blocked ld_preload (lowercase)", {
        "image": "nginx",
        "env": {"ld_preload": "/tmp/evil.so"}
    })

    expect_invalid("env blocked FRED_TOKEN", {
        "image": "nginx",
        "env": {"FRED_TOKEN": "abc"}
    })

    expect_invalid("env blocked fred_internal (lowercase)", {
        "image": "nginx",
        "env": {"fred_internal": "abc"}
    })

    expect_invalid("env blocked DOCKER_HOST", {
        "image": "nginx",
        "env": {"DOCKER_HOST": "tcp://..."}
    })

    expect_invalid("env blocked docker_host (lowercase)", {
        "image": "nginx",
        "env": {"docker_host": "tcp://..."}
    })

    expect_invalid("env empty key", {
        "image": "nginx",
        "env": {"": "val"}
    })

    expect_invalid("label with fred. prefix", {
        "image": "nginx",
        "labels": {"fred.lease": "abc"}
    })

    expect_invalid("health_check empty test", {
        "image": "nginx",
        "health_check": {"test": []}
    })

    expect_invalid("health_check negative retries", {
        "image": "nginx",
        "health_check": {"test": ["CMD", "true"], "retries": -1}
    })

    expect_invalid("tmpfs more than 4", {
        "image": "nginx",
        "tmpfs": ["/a", "/b", "/c", "/d", "/e"]
    })

    expect_invalid("tmpfs relative path", {
        "image": "nginx",
        "tmpfs": ["var/cache"]
    })

    expect_invalid("user empty group after colon", {
        "image": "nginx",
        "user": "1000:"
    })

    expect_invalid("user empty user before colon", {
        "image": "nginx",
        "user": ":1000"
    })

    expect_invalid("user with whitespace", {
        "image": "nginx",
        "user": "my user"
    })

    expect_invalid("expose port zero", {
        "image": "nginx",
        "expose": ["0"]
    })

    expect_invalid("expose port exceeds 65535", {
        "image": "nginx",
        "expose": ["70000"]
    })

    expect_invalid("expose non-numeric", {
        "image": "nginx",
        "expose": ["abc"]
    })

    # ---------------------------------------------------------------
    # Stack manifests — valid
    # ---------------------------------------------------------------
    expect_valid("minimal stack", {
        "services": {
            "web": {"image": "nginx"}
        }
    })

    expect_valid("stack with depends_on service_started", {
        "services": {
            "web": {
                "image": "nginx",
                "depends_on": {"db": {"condition": "service_started"}}
            },
            "db": {"image": "postgres"}
        }
    })

    expect_valid("stack with depends_on service_healthy", {
        "services": {
            "web": {
                "image": "nginx",
                "depends_on": {"db": {"condition": "service_healthy"}}
            },
            "db": {
                "image": "postgres",
                "health_check": {"test": ["CMD", "pg_isready"]}
            }
        }
    })

    expect_valid("stack diamond dependencies", {
        "services": {
            "a": {
                "image": "img",
                "depends_on": {
                    "b": {"condition": "service_started"},
                    "c": {"condition": "service_started"}
                }
            },
            "b": {
                "image": "img",
                "depends_on": {"d": {"condition": "service_started"}}
            },
            "c": {
                "image": "img",
                "depends_on": {"d": {"condition": "service_started"}}
            },
            "d": {"image": "img"}
        }
    })

    expect_valid("stack service name single char", {
        "services": {"a": {"image": "nginx"}}
    })

    expect_valid("stack service name with hyphens", {
        "services": {"my-web-app": {"image": "nginx"}}
    })

    expect_valid("stack service with expose (inter-service port)", {
        "services": {
            "api": {
                "image": "myapp",
                "expose": ["3000"]
            }
        }
    })

    # ---------------------------------------------------------------
    # Stack manifests — invalid
    # ---------------------------------------------------------------
    expect_invalid("stack empty services", {
        "services": {}
    })

    expect_invalid("stack service name uppercase", {
        "services": {"Web": {"image": "nginx"}}
    })

    expect_invalid("stack service name with underscore", {
        "services": {"my_db": {"image": "nginx"}}
    })

    expect_invalid("stack service name leading hyphen", {
        "services": {"-web": {"image": "nginx"}}
    })

    expect_invalid("stack service name trailing hyphen", {
        "services": {"web-": {"image": "nginx"}}
    })

    expect_invalid("stack service name with dot", {
        "services": {"my.svc": {"image": "nginx"}}
    })

    expect_invalid("stack service name too long (64 chars)", {
        "services": {"a" * 64: {"image": "nginx"}}
    })

    expect_invalid("stack service missing image", {
        "services": {"web": {}}
    })

    expect_invalid("stack unknown top-level field", {
        "services": {"web": {"image": "nginx"}},
        "version": "3"
    })

    expect_invalid("stack depends_on invalid condition", {
        "services": {
            "web": {
                "image": "nginx",
                "depends_on": {"db": {"condition": "service_completed"}}
            },
            "db": {"image": "postgres"}
        }
    })

    expect_invalid("stack depends_on empty condition", {
        "services": {
            "web": {
                "image": "nginx",
                "depends_on": {"db": {"condition": ""}}
            },
            "db": {"image": "postgres"}
        }
    })

    # ---------------------------------------------------------------
    # Edge cases — valid
    # ---------------------------------------------------------------
    expect_valid("port 1/tcp (minimum)", {
        "image": "nginx",
        "ports": {"1/tcp": {}}
    })

    expect_valid("port 65535/tcp (maximum)", {
        "image": "nginx",
        "ports": {"65535/tcp": {}}
    })

    expect_valid("host_port 0 (auto-assign)", {
        "image": "nginx",
        "ports": {"80/tcp": {"host_port": 0}}
    })

    expect_valid("expose port 1 (minimum)", {
        "image": "nginx",
        "expose": ["1"]
    })

    expect_valid("expose port 65535 (maximum)", {
        "image": "nginx",
        "expose": ["65535"]
    })

    expect_valid("stack service name max length (63 chars)", {
        "services": {"a" * 63: {"image": "nginx"}}
    })

    expect_valid("tmpfs exactly 4 mounts (max)", {
        "image": "nginx",
        "tmpfs": ["/a", "/b", "/c", "/d"]
    })

    expect_valid("tmpfs sub-path of /run (allowed)", {
        "image": "nginx",
        "tmpfs": ["/run/mysqld"]
    })

    # ---------------------------------------------------------------
    # Edge cases — invalid
    # ---------------------------------------------------------------
    expect_invalid("port 65536/tcp (exceeds max)", {
        "image": "nginx",
        "ports": {"65536/tcp": {}}
    })

    expect_invalid("expose port 65536 (exceeds max)", {
        "image": "nginx",
        "expose": ["65536"]
    })

    expect_invalid("not a manifest (random object)", {
        "foo": "bar"
    })

    expect_invalid("not a manifest (array)", [])

    expect_invalid("not a manifest (string)", "nginx")

    expect_invalid("not a manifest (null)", None)

    # ---------------------------------------------------------------
    # Schema drift regression tests
    # These cover the five categories of drift between JSON Schema
    # and Go runtime validation identified during review.
    # ---------------------------------------------------------------

    # (a) health_check.test — CMD/CMD-SHELL require ≥2 items, NONE exactly 1
    expect_invalid("health_check CMD without args", {
        "image": "nginx",
        "health_check": {"test": ["CMD"]}
    })

    expect_invalid("health_check CMD-SHELL without args", {
        "image": "nginx",
        "health_check": {"test": ["CMD-SHELL"]}
    })

    expect_invalid("health_check NONE with extra args", {
        "image": "nginx",
        "health_check": {"test": ["NONE", "extra"]}
    })

    expect_invalid("health_check unknown test type", {
        "image": "nginx",
        "health_check": {"test": ["EXEC", "true"]}
    })

    expect_valid("health_check CMD with multiple args", {
        "image": "nginx",
        "health_check": {"test": ["CMD", "curl", "-f", "http://localhost/"]}
    })

    expect_valid("health_check CMD-SHELL with one arg", {
        "image": "nginx",
        "health_check": {"test": ["CMD-SHELL", "curl -f http://localhost/ || exit 1"]}
    })

    # (b) stop_grace_period — integer nanosecond bounds (1s–120s)
    expect_invalid("stop_grace_period integer below 1s", {
        "image": "nginx",
        "stop_grace_period": 999999999
    })

    expect_invalid("stop_grace_period integer above 120s", {
        "image": "nginx",
        "stop_grace_period": 120000000001
    })

    expect_valid("stop_grace_period integer exactly 1s", {
        "image": "nginx",
        "stop_grace_period": 1000000000
    })

    expect_valid("stop_grace_period integer exactly 120s", {
        "image": "nginx",
        "stop_grace_period": 120000000000
    })

    expect_valid("stop_grace_period string 10s", {
        "image": "nginx",
        "stop_grace_period": "10s"
    })

    expect_valid("stop_grace_period string compound 1m30s", {
        "image": "nginx",
        "stop_grace_period": "1m30s"
    })

    # (c) tmpfs — blocked paths enforced
    expect_invalid("tmpfs root path /", {
        "image": "nginx",
        "tmpfs": ["/"]
    })

    expect_invalid("tmpfs /tmp (auto-mounted)", {
        "image": "nginx",
        "tmpfs": ["/tmp"]
    })

    expect_invalid("tmpfs /run (auto-mounted)", {
        "image": "nginx",
        "tmpfs": ["/run"]
    })

    expect_invalid("tmpfs /proc sub-path", {
        "image": "nginx",
        "tmpfs": ["/proc/self"]
    })

    expect_invalid("tmpfs /sys sub-path", {
        "image": "nginx",
        "tmpfs": ["/sys/fs"]
    })

    expect_invalid("tmpfs /dev sub-path", {
        "image": "nginx",
        "tmpfs": ["/dev/shm"]
    })

    expect_invalid("tmpfs /proc exact", {
        "image": "nginx",
        "tmpfs": ["/proc"]
    })

    expect_invalid("tmpfs /sys exact", {
        "image": "nginx",
        "tmpfs": ["/sys"]
    })

    expect_invalid("tmpfs /dev exact", {
        "image": "nginx",
        "tmpfs": ["/dev"]
    })

    expect_valid("tmpfs /var/log (allowed path)", {
        "image": "nginx",
        "tmpfs": ["/var/log"]
    })

    expect_valid("tmpfs /tmp-data (not /tmp exact)", {
        "image": "nginx",
        "tmpfs": ["/tmp-data"]
    })

    expect_valid("tmpfs /run/mysqld (sub-path of /run allowed)", {
        "image": "nginx",
        "tmpfs": ["/run/mysqld"]
    })

    # (d) Uppercase protocols accepted (Go normalizes to lowercase)
    expect_valid("port uppercase TCP", {
        "image": "nginx",
        "ports": {"80/TCP": {}}
    })

    expect_valid("port uppercase UDP", {
        "image": "nginx",
        "ports": {"53/UDP": {}}
    })

    expect_valid("port mixed case Tcp", {
        "image": "nginx",
        "ports": {"80/Tcp": {}}
    })

    # (e) Empty depends_on allowed in single-service manifest
    expect_valid("single-service with empty depends_on", {
        "image": "nginx",
        "depends_on": {}
    })

    # ---------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------
    total = passed + failed
    print(f"\n{passed}/{total} passed, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
