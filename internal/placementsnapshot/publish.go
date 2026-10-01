package placementsnapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// stagedFile is one closed temporary file and the inode it was checked as.
type stagedFile struct {
	temp string
	info os.FileInfo
}

// verifiedSet is minted only by verifyStaged: both staged copies were re-read
// at the receipt's sizes and digests and passed bbolt's consistency check. A
// manifest can be built only from one.
type verifiedSet struct {
	receipt    placement.CutReceipt
	placements stagedFile
	payloads   stagedFile
}

// publish streams cut into a new set named for at and publishes it, manifest
// last. On failure it removes every file the attempt created.
func (snapshots *Directory) publish(
	ctx context.Context,
	cut placement.ConsistentCut,
	at time.Time,
) (setName, error) {
	set, err := newSetName(at)
	if err != nil {
		return setName{}, errors.Join(err, cut.Discard())
	}
	if err := snapshots.publishAs(ctx, cut, set); err != nil {
		return setName{}, err
	}
	return set, nil
}

func (snapshots *Directory) publishAs(
	ctx context.Context,
	cut placement.ConsistentCut,
	set setName,
) (resultErr error) {
	attempt := &publication{snapshots: snapshots, open: make(map[string]*os.File)}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, attempt.abandon())
		}
	}()
	placementsTemp, placementsFile, err := attempt.createTemp()
	if err != nil {
		return errors.Join(err, cut.Discard())
	}
	payloadsTemp, payloadsFile, err := attempt.createTemp()
	if err != nil {
		return errors.Join(err, cut.Discard())
	}
	receipt, err := cut.Stream(ctx, placementsFile, payloadsFile)
	if err != nil {
		return err
	}
	if err := errors.Join(ctx.Err(), attempt.seal(placementsTemp), attempt.seal(payloadsTemp)); err != nil {
		return err
	}
	verified, err := snapshots.verifyStaged(ctx, receipt, placementsTemp, payloadsTemp)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := attempt.publish(verified.placements, snapshots.names.file(set, fileKindPlacements)); err != nil {
		return err
	}
	if err := attempt.publish(verified.payloads, snapshots.names.file(set, fileKindPayloads)); err != nil {
		return err
	}
	if err := snapshots.directory.Sync(); err != nil {
		return fmt.Errorf("sync snapshot directory: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	encoded, err := snapshots.names.newManifest(set, verified).encode()
	if err != nil {
		return err
	}
	manifestTemp, manifestFile, err := attempt.createTemp()
	if err != nil {
		return err
	}
	if _, err := manifestFile.Write(encoded); err != nil {
		return fmt.Errorf("write snapshot manifest: %w", err)
	}
	if err := attempt.seal(manifestTemp); err != nil {
		return err
	}
	manifestInfo, err := snapshots.directory.Lstat(manifestTemp)
	if err != nil {
		return fmt.Errorf("stat staged snapshot manifest: %w", err)
	}
	if !snapshots.private(manifestInfo) || manifestInfo.Size() != int64(len(encoded)) {
		return errors.New("staged snapshot manifest is not the private file just written")
	}
	if err := attempt.publish(
		stagedFile{temp: manifestTemp, info: manifestInfo},
		snapshots.names.file(set, fileKindManifest),
	); err != nil {
		return err
	}
	if err := snapshots.directory.Sync(); err != nil {
		return fmt.Errorf("sync snapshot directory: %w", err)
	}
	return nil
}

// publication tracks one attempt's files so a failed attempt removes exactly
// what it created.
type publication struct {
	snapshots *Directory
	open      map[string]*os.File
	temps     []string
	published []stagedFile // final names, in publication order
}

func (attempt *publication) createTemp() (string, *os.File, error) {
	name, file, err := attempt.snapshots.directory.CreateTemp(attempt.snapshots.names.tempPrefix(), 0o600)
	if err != nil {
		return "", nil, fmt.Errorf("create staged snapshot file: %w", err)
	}
	attempt.temps = append(attempt.temps, name)
	attempt.open[name] = file
	if err := file.Chmod(0o600); err != nil {
		return "", nil, fmt.Errorf("set staged snapshot file permissions: %w", err)
	}
	return name, file, nil
}

// seal makes a staged file's contents durable and closes it.
func (attempt *publication) seal(temp string) error {
	file := attempt.open[temp]
	if file == nil {
		return fmt.Errorf("staged snapshot file %s is not open", temp)
	}
	delete(attempt.open, temp)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return fmt.Errorf("seal staged snapshot file: %w", err)
	}
	return nil
}

// publish moves a checked staged file to its final name, which must be absent,
// and proves the name now holds the checked inode.
func (attempt *publication) publish(staged stagedFile, final string) error {
	directory := attempt.snapshots.directory
	if err := directory.RenameNoReplace(staged.temp, final); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("snapshot file %s already exists", final)
		}
		return fmt.Errorf("publish snapshot file %s: %w", final, err)
	}
	attempt.temps = slices.DeleteFunc(attempt.temps, func(name string) bool { return name == staged.temp })
	attempt.published = append(attempt.published, stagedFile{temp: final, info: staged.info})
	info, err := directory.Lstat(final)
	if err != nil {
		return fmt.Errorf("stat published snapshot file %s: %w", final, err)
	}
	if !os.SameFile(info, staged.info) {
		return fmt.Errorf("published snapshot file %s is not the checked copy", final)
	}
	return nil
}

// abandon removes everything a failed attempt created: published files newest
// first, so a manifest goes before the data it describes, then staged files.
// If a published file cannot be removed, every older one is kept, so a manifest
// never outlives the data it names.
func (attempt *publication) abandon() error {
	directory := attempt.snapshots.directory
	var errs []error
	for name, file := range attempt.open {
		if err := file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close staged snapshot file %s: %w", name, err))
		}
	}
	for i := len(attempt.published) - 1; i >= 0; i-- {
		if err := attempt.unpublish(attempt.published[i]); err != nil {
			errs = append(errs, err)
			break
		}
	}
	for _, temp := range attempt.temps {
		if err := directory.Remove(temp); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, fmt.Errorf("remove staged snapshot file %s: %w", temp, err))
		}
	}
	return errors.Join(errs...)
}

// unpublish removes one file this attempt published and makes the removal
// durable before anything older is touched.
func (attempt *publication) unpublish(final stagedFile) error {
	directory := attempt.snapshots.directory
	info, err := directory.Lstat(final.temp)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("stat published snapshot file %s: %w", final.temp, err)
	case !os.SameFile(info, final.info):
		return fmt.Errorf("published snapshot file %s was replaced; leaving it and older files", final.temp)
	}
	if err := directory.Remove(final.temp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove published snapshot file %s: %w", final.temp, err)
	}
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync snapshot directory: %w", err)
	}
	return nil
}

// verifyStaged re-reads both staged copies against the receipt.
func (snapshots *Directory) verifyStaged(
	ctx context.Context,
	receipt placement.CutReceipt,
	placementsTemp, payloadsTemp string,
) (verifiedSet, error) {
	if !receipt.Valid() {
		return verifiedSet{}, errors.New("snapshot receipt is invalid")
	}
	if receipt.ProviderUUID() != snapshots.names.providerUUID {
		return verifiedSet{}, errors.New("snapshot receipt names another provider")
	}
	placementsInfo, err := snapshots.verifyCopy(ctx, placementsTemp, receipt.Placements())
	if err != nil {
		return verifiedSet{}, fmt.Errorf("verify placements copy: %w", err)
	}
	payloadsInfo, err := snapshots.verifyCopy(ctx, payloadsTemp, receipt.Payloads())
	if err != nil {
		return verifiedSet{}, fmt.Errorf("verify payloads copy: %w", err)
	}
	return verifiedSet{
		receipt:    receipt,
		placements: stagedFile{temp: placementsTemp, info: placementsInfo},
		payloads:   stagedFile{temp: payloadsTemp, info: payloadsInfo},
	}, nil
}

// verifyCopy proves a staged file is a private regular file holding exactly
// the copied bytes, and that those bytes are a consistent bbolt database.
func (snapshots *Directory) verifyCopy(
	ctx context.Context,
	temp string,
	want placement.CutCopy,
) (os.FileInfo, error) {
	file, err := snapshots.directory.OpenFile(temp, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !snapshots.private(info) {
		return nil, errors.New("staged copy is not a private regular file")
	}
	if info.Size() != want.Size {
		return nil, fmt.Errorf("staged copy is %d bytes, the stream copied %d", info.Size(), want.Size)
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, contextReader{ctx: ctx, reader: file}); err != nil {
		return nil, fmt.Errorf("re-read staged copy: %w", err)
	}
	if !bytes.Equal(hash.Sum(nil), want.SHA256[:]) {
		return nil, errors.New("staged copy does not match the streamed digest")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := snapshots.checkBolt(temp, info); err != nil {
		return nil, err
	}
	return info, nil
}

// checkBolt runs bbolt's page and freelist checker over a staged copy opened
// read-only through the directory, never by path.
func (snapshots *Directory) checkBolt(temp string, expected os.FileInfo) (resultErr error) {
	db, err := bolt.Open(snapshots.directory.DisplayPath(temp), 0o600, &bolt.Options{
		ReadOnly: true,
		Timeout:  time.Second,
		OpenFile: func(_ string, flag int, mode os.FileMode) (*os.File, error) {
			file, err := snapshots.directory.OpenFile(temp, flag&^os.O_CREATE, mode)
			if err != nil {
				return nil, err
			}
			info, err := file.Stat()
			if err != nil || !os.SameFile(info, expected) {
				_ = file.Close()
				return nil, errors.New("staged copy changed before its consistency check")
			}
			return file, nil
		},
	})
	if err != nil {
		return fmt.Errorf("open staged copy: %w", err)
	}
	defer func() {
		if closeErr := db.Close(); resultErr == nil && closeErr != nil {
			resultErr = fmt.Errorf("close staged copy: %w", closeErr)
		}
	}()
	return db.View(func(tx *bolt.Tx) error {
		var (
			first error
			count int
		)
		for checkErr := range tx.Check() {
			if first == nil {
				first = checkErr
			}
			count++
		}
		if count > 0 {
			return fmt.Errorf("staged copy failed bbolt's consistency check (%d errors, first: %w)", count, first)
		}
		return nil
	})
}

// contextReader stops a long re-read when ctx ends.
type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
