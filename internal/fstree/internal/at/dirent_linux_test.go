//go:build linux

package at

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// encodeDirent builds one linux_dirent64 record the way the kernel does:
// header, NUL-terminated name, zero padding to 8 bytes.
func encodeDirent(ino uint64, off int64, typ uint8, name string) []byte {
	reclen := (direntHeader + len(name) + 1 + direntAlign - 1) / direntAlign * direntAlign
	rec := make([]byte, reclen)
	binary.NativeEndian.PutUint64(rec[0:8], ino)
	binary.NativeEndian.PutUint64(rec[8:16], uint64(off))
	binary.NativeEndian.PutUint16(rec[16:18], uint16(reclen))
	rec[18] = typ
	copy(rec[direntHeader:], name)
	return rec
}

func encodeDirents(names ...string) []byte {
	var buf []byte
	for i, name := range names {
		buf = append(buf, encodeDirent(uint64(100+i), int64(i+1), unix.DT_REG, name)...)
	}
	return buf
}

// withReclen rewrites the d_reclen of the record at the start of rec.
func withReclen(rec []byte, reclen uint16) []byte {
	out := slices.Clone(rec)
	binary.NativeEndian.PutUint16(out[16:18], reclen)
	return out
}

func TestParseDirents(t *testing.T) {
	long := strings.Repeat("n", MaxNameLen)
	t.Run("valid records in order, dots skipped", func(t *testing.T) {
		buf := encodeDirents(".", "..", "a", long, "...", "\xff\n")
		out, next, err := parseDirents(buf, batchSize, nil)
		require.NoError(t, err)
		require.Equal(t, []dirent{
			{name: "a", off: 3, typ: unix.DT_REG},
			{name: long, off: 4, typ: unix.DT_REG},
			{name: "...", off: 5, typ: unix.DT_REG},
			{name: "\xff\n", off: 6, typ: unix.DT_REG},
		}, out)
		require.Equal(t, int64(6), next)
	})
	t.Run("stops at the limit and resumes after the last name taken", func(t *testing.T) {
		out, next, err := parseDirents(encodeDirents("a", "b", "c"), 2, nil)
		require.NoError(t, err)
		require.Len(t, out, 2)
		require.Equal(t, int64(2), next)
	})
	t.Run("dots alone advance the cookie", func(t *testing.T) {
		out, next, err := parseDirents(encodeDirents(".", ".."), batchSize, nil)
		require.NoError(t, err)
		require.Empty(t, out)
		require.Equal(t, int64(2), next)
	})
	t.Run("appends to out", func(t *testing.T) {
		out, _, err := parseDirents(encodeDirents("b"), batchSize, []dirent{{name: "a"}})
		require.NoError(t, err)
		require.Equal(t, []string{"a", "b"}, []string{out[0].name, out[1].name})
	})

	valid := encodeDirent(1, 1, unix.DT_REG, "name")
	malformed := map[string][]byte{
		"truncated header":       valid[:direntHeader-1],
		"zero record length":     withReclen(valid, 0),
		"short record length":    withReclen(valid, direntHeader-3),
		"record past the buffer": withReclen(valid, uint16(len(valid)+8)),
		"unaligned length":       withReclen(valid, uint16(len(valid)-1)),
		"unterminated name": func() []byte {
			rec := slices.Clone(valid)
			for i := direntHeader; i < len(rec); i++ {
				rec[i] = 'x'
			}
			return rec
		}(),
		"empty name":           encodeDirent(1, 1, unix.DT_REG, ""),
		"slash in name":        encodeDirent(1, 1, unix.DT_REG, "a/b"),
		"name past NAME_MAX":   encodeDirent(1, 1, unix.DT_REG, long+"n"),
		"garbage after record": append(slices.Clone(valid), 1, 2, 3),
	}
	for name, buf := range malformed {
		t.Run(name, func(t *testing.T) {
			out, _, err := parseDirents(buf, batchSize, nil)
			require.ErrorIs(t, err, ErrMalformedDirent)
			for _, entry := range out {
				require.True(t, validName(entry.name))
			}
		})
	}
}

// The parser agrees with the kernel: every name a real getdents64 returns
// comes back, and each entry's cookie resumes the listing right after it.
func TestParseDirentsReadsRealGetdentsOutput(t *testing.T) {
	dir := tempDir(t)
	names := []string{"a", "b c", "line\nbreak", "\xff\xfe", ".hidden", "...", strings.Repeat("L", MaxNameLen)}
	for _, name := range names {
		writeFile(t, filepath.Join(dir, name), "")
	}
	d := openDir(t, dir)
	fd := int(d.Fd())
	buf := make([]byte, direntBufSize)
	n, err := unix.Getdents(fd, buf)
	require.NoError(t, err)
	entries, _, err := parseDirents(buf[:n], batchSize, nil)
	require.NoError(t, err)
	var got []string
	for _, entry := range entries {
		got = append(got, entry.name)
	}
	require.ElementsMatch(t, names, got)

	for i, entry := range entries {
		require.NoError(t, seekDir(fd, entry.off))
		n, err := unix.Getdents(fd, buf)
		require.NoError(t, err)
		rest, _, err := parseDirents(buf[:n], batchSize, nil)
		require.NoError(t, err)
		if want := entries[i+1:]; len(want) > 0 {
			require.Equal(t, want, rest, "resuming after %q", entry.name)
		} else {
			require.Empty(t, rest, "resuming after the last entry")
		}
	}
}

// A read that returns only "." and ".." is followed by another; the reader
// does not mistake it for an empty directory.
func TestReaderReadsPastADotOnlyRead(t *testing.T) {
	dir := tempDir(t)
	writeFile(t, filepath.Join(dir, "a"), "")
	writeFile(t, filepath.Join(dir, "b"), "")
	d := openDir(t, dir)
	reader := NewReader()
	// Room for exactly the two 24-byte dot records.
	reader.size = 2 * 24

	// The first getdents64 can hold only the dots, so a non-empty first
	// batch proves the reader went on to the next one.
	entries, next, eof, err := reader.read(int(d.Fd()), 0)
	require.NoError(t, err)
	require.False(t, eof)
	require.NotEmpty(t, entries)
	var names []string
	for !eof {
		for _, entry := range entries {
			names = append(names, entry.name)
		}
		entries, next, eof, err = reader.read(int(d.Fd()), next)
		require.NoError(t, err)
	}
	require.ElementsMatch(t, []string{"a", "b"}, names)

	empty := openDir(t, tempDir(t))
	entries, _, eof, err = reader.read(int(empty.Fd()), 0)
	require.NoError(t, err)
	require.True(t, eof)
	require.Empty(t, entries)
}

func TestReaderSizes(t *testing.T) {
	reader := NewReader()
	require.Equal(t, direntBufSize, reader.size)
	reader.Shrink()
	require.Equal(t, minDirentRead, reader.size)
	for range 10 {
		reader.Grow()
	}
	require.Equal(t, direntBufSize, reader.size)
	reader.Shrink()
	reader.Full()
	require.Equal(t, direntBufSize, reader.size)
}

// ReadBatch binds every entry to the directory it read, for both sides, and
// reports a directory removed while held as ENOENT.
func TestReadBatchBindsEntriesToTheirDirectory(t *testing.T) {
	parentPath := tempDir(t)
	require.NoError(t, os.Mkdir(filepath.Join(parentPath, "dir"), 0o755))
	writeFile(t, filepath.Join(parentPath, "dir", "a"), "")
	writeFile(t, filepath.Join(parentPath, "dir", "b"), "")
	d := openOwned(t, parentPath, "dir")
	reader := NewReader()

	listed, _, eof, err := d.ReadBatch(&reader, 0)
	require.NoError(t, err)
	require.False(t, eof)
	var names []string
	for _, entry := range listed {
		require.Same(t, d, entry.dir)
		names = append(names, entry.String())
	}
	require.ElementsMatch(t, []string{"a", "b"}, names)

	viewed, next, eof, err := d.View().ReadBatch(&reader, 0)
	require.NoError(t, err)
	require.False(t, eof)
	require.Len(t, viewed, 2)
	for _, entry := range viewed {
		require.Same(t, d, entry.dir)
	}
	_, _, eof, err = d.View().ReadBatch(&reader, next)
	require.NoError(t, err)
	require.True(t, eof)

	require.NoError(t, os.RemoveAll(filepath.Join(parentPath, "dir")))
	_, _, _, err = d.ReadBatch(&reader, 0)
	require.ErrorIs(t, err, unix.ENOENT)
}

func TestValidName(t *testing.T) {
	for _, name := range []string{"a", "...", ".a", "a.", " ", "\xff", strings.Repeat("n", MaxNameLen)} {
		require.True(t, validName(name), "%q", name)
		require.True(t, validName([]byte(name)), "%q", name)
	}
	for _, name := range []string{"", ".", "..", "/", "a/b", "a/", "/a", "a\x00", strings.Repeat("n", MaxNameLen+1)} {
		require.False(t, validName(name), "%q", name)
		require.False(t, validName([]byte(name)), "%q", name)
	}
}

// FuzzParseDirents: no input makes the parser panic, and every name it
// returns is one path component that cannot resolve outside its directory.
func FuzzParseDirents(f *testing.F) {
	valid := encodeDirent(1, 1, unix.DT_REG, "name")
	f.Add(encodeDirents(".", "..", "a", "b"), uint16(batchSize))
	f.Add(encodeDirents("a", strings.Repeat("n", MaxNameLen), "\xff"), uint16(1))
	f.Add(encodeDirents(".", ".."), uint16(batchSize))
	f.Add(valid[:direntHeader-1], uint16(batchSize))
	f.Add(withReclen(valid, 0), uint16(batchSize))
	f.Add(withReclen(valid, uint16(len(valid)+8)), uint16(batchSize))
	f.Add(withReclen(valid, uint16(len(valid)-1)), uint16(batchSize))
	f.Add(encodeDirent(1, 1, unix.DT_REG, "a/b"), uint16(batchSize))
	f.Add(encodeDirent(1, 1, unix.DT_REG, ""), uint16(batchSize))
	f.Add(append(slices.Clone(valid), 0xff), uint16(batchSize))
	f.Add([]byte{}, uint16(batchSize))

	// Real kernel output for a directory with awkward names.
	dir := f.TempDir()
	for _, name := range []string{"a", "line\nbreak", "\xff", strings.Repeat("L", MaxNameLen)} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o644); err != nil {
			f.Fatal(err)
		}
	}
	d, err := os.Open(dir)
	if err != nil {
		f.Fatal(err)
	}
	buf := make([]byte, direntBufSize)
	n, err := unix.Getdents(int(d.Fd()), buf)
	_ = d.Close()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(buf[:n], uint16(batchSize))

	f.Fuzz(func(t *testing.T, buf []byte, limit uint16) {
		out, _, err := parseDirents(buf, int(limit), nil)
		require.LessOrEqual(t, len(out), int(limit))
		for _, entry := range out {
			require.NotEmpty(t, entry.name)
			require.LessOrEqual(t, len(entry.name), MaxNameLen)
			require.NotContains(t, entry.name, "/")
			require.NotContains(t, entry.name, "\x00")
			require.False(t, isDotName(entry.name))
		}
		if err == nil {
			// A buffer that parses whole re-encodes to records holding the
			// same names in the same order.
			var names []string
			for _, entry := range out {
				names = append(names, entry.name)
			}
			again, _, err := parseDirents(encodeDirents(names...), int(limit), nil)
			require.NoError(t, err)
			require.Len(t, again, len(out))
			for i := range out {
				require.Equal(t, out[i].name, again[i].name)
			}
		}
	})
}
