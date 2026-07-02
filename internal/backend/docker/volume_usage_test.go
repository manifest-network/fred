package docker

import (
	"context"
	"errors"
	"testing"
)

func TestNoopVolumeManager_Usage_Unsupported(t *testing.T) {
	n := &noopVolumeManager{}
	_, err := n.Usage(context.Background(), "fred-x-app-0")
	if !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("noop Usage error = %v; want wraps errors.ErrUnsupported", err)
	}
}

func TestNoopVolumeManager_Kind(t *testing.T) {
	if got := (&noopVolumeManager{}).Kind(); got != "noop" {
		t.Fatalf("noop Kind = %q; want %q", got, "noop")
	}
}

func TestParseBtrfsSubvolumeID(t *testing.T) {
	out := "fred-x-app-0\n\tName: \t\t\tfred-x-app-0\n\tUUID: \t\t\t...\n\tSubvolume ID: \t\t258\n\tGeneration: \t\t9\n"
	id, err := parseBtrfsSubvolumeID(out)
	if err != nil || id != 258 {
		t.Fatalf("parseBtrfsSubvolumeID = %d, %v; want 258, nil", id, err)
	}
}

func TestParseBtrfsQgroupRfer(t *testing.T) {
	// `btrfs qgroup show --raw --sync` columns: qgroupid  rfer  excl
	out := "qgroupid         rfer         excl\n--------         ----         ----\n0/5             16384        16384\n0/258      1073741824   1073741824\n"
	rfer, excl, err := parseBtrfsQgroupRfer(out, 258)
	if err != nil || rfer != 1073741824 || excl != 1073741824 {
		t.Fatalf("parseBtrfsQgroupRfer = %d,%d,%v; want 1073741824,1073741824,nil", rfer, excl, err)
	}
}

func TestParseBtrfsQgroupRfer_NotFound(t *testing.T) {
	out := "qgroupid rfer excl\n0/5 16384 16384\n"
	if _, _, err := parseBtrfsQgroupRfer(out, 999); err == nil {
		t.Fatalf("expected error for missing qgroup id")
	}
}

func TestParseXfsReportUsedBlocks(t *testing.T) {
	// `xfs_quota -x -c "report -p -b -N" <mnt>` rows: <projid> <used> <soft> <hard> <warn/grace>
	// Used is in 1KiB blocks.
	out := "#0            0          0          0     00 [--------]\n" +
		"#1048576   1024          0    2097152     00 [--------]\n"
	blocks, err := parseXfsReportUsedBlocks(out, 1048576)
	if err != nil || blocks != 1024 {
		t.Fatalf("parseXfsReportUsedBlocks = %d, %v; want 1024, nil", blocks, err)
	}
}

func TestParseXfsReportUsedBlocks_NotFound(t *testing.T) {
	if _, err := parseXfsReportUsedBlocks("#0 0 0 0 00 [---]\n", 42); err == nil {
		t.Fatalf("expected error for missing project id")
	}
}

func TestParseZfsReferenced(t *testing.T) {
	v, err := parseZfsReferenced("1073741824\n")
	if err != nil || v != 1073741824 {
		t.Fatalf("parseZfsReferenced = %d, %v; want 1073741824, nil", v, err)
	}
}
