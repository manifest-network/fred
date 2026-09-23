package imagefetch

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	maxLayerEntries      = 131072
	maxPathBytes         = 4096
	maxHeaderBytes       = 64 << 10
	maxRetainedPathBytes = 32 << 20
	maxResolvedPathBytes = 64 << 20
	metadataAllocation   = int64(16 << 10)
)

type layerBudget struct {
	remaining                                int64
	allocated                                int64
	entries, nodes, pathBytes, resolvedBytes int
	layer                                    int
	root                                     *layerNode
}

// layerNode models the actual image namespace, including inherited aliases.
// A hardlink's authority is its resolved regular node and originating layer.
// Lexical archive names never authorize a link to unaccounted lower-layer data.
type layerNode struct {
	kind                               byte
	size                               int64
	target                             string
	owner, descendants, allocatedLayer int
	children                           map[string]*layerNode
}

type layerTree struct {
	budget *layerBudget
	seen   map[string]bool
}

func inspectLayer(ctx context.Context, file *os.File, mediaType string, diffID digest.Digest, budget *layerBudget) error {
	if err := diffID.Validate(); err != nil || diffID.Algorithm() != digest.SHA256 {
		return errors.New("invalid uncompressed layer digest")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	input := bufio.NewReader(contextReader{ctx, file})
	var raw io.Reader
	var closeDecoder func()
	switch mediaType {
	case ocispec.MediaTypeImageLayer, "application/vnd.docker.image.rootfs.diff.tar":
		raw = input
	case ocispec.MediaTypeImageLayerGzip, "application/vnd.docker.image.rootfs.diff.tar.gzip":
		decoder, err := gzip.NewReader(input)
		if err != nil {
			return err
		}
		raw = decoder
		closeDecoder = func() { _ = decoder.Close() }
	case ocispec.MediaTypeImageLayerZstd:
		decoder, err := zstd.NewReader(input, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(64<<20))
		if err != nil {
			return err
		}
		raw = decoder
		closeDecoder = decoder.Close
	default:
		return fmt.Errorf("unsupported image layer media type %q", mediaType)
	}
	if closeDecoder != nil {
		defer closeDecoder()
	}
	bounded := &budgetReader{reader: raw, remaining: budget.remaining}
	hash := sha256.New()
	stream := io.TeeReader(bounded, hash)
	reader := tar.NewReader(stream)
	if budget.root == nil {
		budget.root = &layerNode{kind: tar.TypeDir, children: make(map[string]*layerNode)}
	}
	budget.layer++
	tree := layerTree{budget: budget, seen: make(map[string]bool)}
	logical := int64(0)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		budget.entries++
		if budget.entries > maxLayerEntries {
			return errors.New("image layers exceed entry budget")
		}
		name, err := safeLayerPath(header.Name)
		if err != nil {
			return err
		}
		if tree.seen[name] {
			return errors.New("image layer contains duplicate paths")
		}
		if err := tree.accountName(name); err != nil {
			return err
		}
		tree.seen[name] = true
		if header.Size < 0 || header.Size > budget.remaining-logical {
			return errors.New("image layer exceeds logical file byte budget")
		}
		logical += header.Size
		metadata := len(header.Name) + len(header.Linkname) + len(header.Uname) + len(header.Gname)
		for key, value := range header.PAXRecords {
			if strings.HasPrefix(key, "SCHILY.xattr.trusted.overlay.") || strings.HasPrefix(key, "SCHILY.xattr.user.overlay.") {
				return errors.New("image layer contains reserved overlay filesystem attributes")
			}
			if strings.HasPrefix(key, "GNU.sparse") {
				return errors.New("sparse image layer entries are unsupported")
			}
			metadata += len(key) + len(value)
		}
		for key, value := range header.Xattrs {
			if strings.HasPrefix(key, "trusted.overlay.") || strings.HasPrefix(key, "user.overlay.") {
				return errors.New("image layer contains reserved overlay filesystem attributes")
			}
			metadata += len(key) + len(value)
		}
		if metadata > maxHeaderBytes {
			return errors.New("image layer header exceeds metadata budget")
		}
		budget.allocated += metadataAllocation + roundBlock(int64(metadata), 4096)
		copied, err := tree.apply(ctx, name, header)
		if err != nil {
			return err
		}
		if copied > budget.remaining-logical {
			return errors.New("image hardlinks exceed expanded byte budget")
		}
		logical += copied
		//nolint:gosec // G110: decoded bytes and logical tar sizes are independently bounded above.
		if _, err := io.Copy(io.Discard, reader); err != nil {
			return err
		}
	}
	// A tar terminator is not a compression terminator. Charge trailing decoded
	// bytes and require the entire compressor checksum and diffID to match.
	if _, err := io.Copy(io.Discard, stream); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != diffID.Encoded() {
		return errors.New("image layer differs from its uncompressed digest")
	}
	consumed := budget.remaining - bounded.remaining
	budget.remaining -= max(consumed, logical)
	return nil
}

func safeLayerPath(value string) (string, error) {
	if value == "" || len(value) > maxPathBytes || !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
		return "", errors.New("image layer path exceeds path budget")
	}
	value = strings.TrimLeft(value, "/")
	for _, component := range strings.Split(value, "/") {
		if component == ".." {
			return "", errors.New("image layer path escapes extraction root")
		}
	}
	return strings.Clone(path.Clean(value)), nil
}

func roundBlock(size, block int64) int64 { return (size + block - 1) / block * block }

func (t *layerTree) accountName(name string) error {
	if len(name) > maxRetainedPathBytes-t.budget.pathBytes {
		return errors.New("image paths exceed retained path byte budget")
	}
	t.budget.pathBytes += len(name)
	return nil
}

func (t *layerTree) newNode(name string, kind byte) (*layerNode, error) {
	if t.budget.nodes >= maxLayerEntries {
		return nil, errors.New("image explicit and implicit paths exceed entry budget")
	}
	if err := t.accountName(name); err != nil {
		return nil, err
	}
	t.budget.nodes++
	t.budget.allocated += metadataAllocation
	node := &layerNode{kind: kind}
	if kind == tar.TypeDir {
		node.children = make(map[string]*layerNode)
	}
	return node, nil
}

// parent resolves symlinks in ancestors exactly against the modeled image root.
// Missing parents may be created only for writes; hardlink source lookup cannot
// invent a directory or regular-file proof. Work and retained nodes are bounded.
func (t *layerTree) parent(ctx context.Context, name string, create bool) ([]*layerNode, string, error) {
	base := path.Base(name)
	pending := strings.Split(path.Dir(name), "/")
	stack := []*layerNode{t.budget.root}
	names := make([]string, 0)
	links := 0
	for len(pending) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		component := pending[0]
		pending = pending[1:]
		if len(component)+1 > maxResolvedPathBytes-t.budget.resolvedBytes {
			return nil, "", errors.New("image symlink resolution exceeds path work budget")
		}
		t.budget.resolvedBytes += len(component) + 1
		switch component {
		case "", ".":
			continue
		case "..":
			if len(stack) > 1 {
				stack = stack[:len(stack)-1]
				names = names[:len(names)-1]
			}
			continue
		}
		parent := stack[len(stack)-1]
		node := parent.children[component]
		if node == nil {
			if !create {
				return nil, "", errors.New("image hardlink source parent is missing")
			}
			var err error
			node, err = t.newNode(path.Join(append(names, component)...), tar.TypeDir)
			if err != nil {
				return nil, "", err
			}
			parent.children[strings.Clone(component)] = node
			for _, ancestor := range stack {
				ancestor.descendants = t.budget.layer
			}
		}
		if node.kind == tar.TypeSymlink {
			links++
			if links > 40 {
				return nil, "", errors.New("image symlink resolution exceeds traversal limit")
			}
			if strings.HasPrefix(node.target, "/") {
				stack = stack[:1]
				names = names[:0]
			}
			pending = append(strings.Split(node.target, "/"), pending...)
			continue
		}
		if node.kind != tar.TypeDir {
			return nil, "", errors.New("image layer traverses a non-directory parent")
		}
		if node.allocatedLayer != t.budget.layer {
			node.allocatedLayer = t.budget.layer
			t.budget.allocated += metadataAllocation
		}
		stack = append(stack, node)
		names = append(names, component)
	}
	return stack, base, nil
}

func (t *layerTree) apply(ctx context.Context, name string, h *tar.Header) (int64, error) {
	if name == "." {
		if h.Typeflag != tar.TypeDir {
			return 0, errors.New("image root must be a directory")
		}
		return 0, nil
	}
	stack, base, err := t.parent(ctx, name, true)
	if err != nil {
		return 0, err
	}
	parent := stack[len(stack)-1]
	if strings.HasPrefix(base, ".wh.") {
		return 0, t.whiteout(parent, base)
	}
	old := parent.children[base]
	if old != nil && old.owner == t.budget.layer {
		return 0, errors.New("image layer aliases an already written path")
	}
	if old != nil && old.kind == tar.TypeDir && h.Typeflag != tar.TypeDir && old.descendants == t.budget.layer {
		return 0, errors.New("image layer replaces a used directory ancestor")
	}
	var size int64
	kind := h.Typeflag
	switch kind {
	case tar.TypeReg, tar.TypeRegA:
		kind = tar.TypeReg
		size = h.Size
	case tar.TypeLink:
		target, err := safeLayerPath(h.Linkname)
		if err != nil {
			return 0, err
		}
		sourceStack, sourceBase, err := t.parent(ctx, target, false)
		if err != nil {
			return 0, err
		}
		source := sourceStack[len(sourceStack)-1].children[sourceBase]
		if source == nil || source.kind != tar.TypeReg || source.owner != t.budget.layer {
			return 0, errors.New("image hardlink must refer to an earlier regular file in the same layer")
		}
		kind = tar.TypeReg
		size = source.size
	case tar.TypeSymlink:
		if h.Linkname == "" || len(h.Linkname) > maxPathBytes || !utf8.ValidString(h.Linkname) || strings.ContainsRune(h.Linkname, 0) {
			return 0, errors.New("image layer symlink exceeds path budget")
		}
		if err := stableSymlinkTarget(h.Linkname); err != nil {
			return 0, err
		}
		if err := t.accountName(h.Linkname); err != nil {
			return 0, err
		}
	case tar.TypeDir, tar.TypeChar, tar.TypeBlock, tar.TypeFifo:
	default:
		return 0, errors.New("unsupported or sparse image layer entry")
	}
	var node *layerNode
	if old != nil && old.kind == tar.TypeDir && kind == tar.TypeDir {
		node = old
	} else {
		node, err = t.newNode(name, kind)
		if err != nil {
			return 0, err
		}
	}
	node.owner = t.budget.layer
	node.size = size
	if kind == tar.TypeSymlink {
		node.target = strings.Clone(h.Linkname)
	}
	parent.children[strings.Clone(base)] = node
	for _, ancestor := range stack {
		ancestor.descendants = t.budget.layer
	}
	t.budget.allocated += roundBlock(size, 4096)
	if h.Typeflag == tar.TypeLink {
		return size, nil
	}
	return 0, nil
}

func (t *layerTree) whiteout(parent *layerNode, base string) error {
	if base == ".wh..wh..opq" {
		if parent.descendants == t.budget.layer {
			return errors.New("opaque whiteout invalidates earlier layer paths")
		}
		parent.children = make(map[string]*layerNode)
		return nil
	}
	target := strings.TrimPrefix(base, ".wh.")
	if target == "" || target == "." || target == ".." {
		return errors.New("invalid image whiteout")
	}
	if node := parent.children[target]; node != nil && (node.owner == t.budget.layer || node.descendants == t.budget.layer) {
		return errors.New("whiteout invalidates an earlier layer path")
	}
	delete(parent.children, target)
	return nil
}

// Docker's classic and containerd extractors differ in whether a target such
// as "alias/../file" is cleaned before or after resolving alias. Admit only
// targets with unambiguous parent traversal: ordinary leading ../ is allowed.
func stableSymlinkTarget(target string) error {
	named := false
	for _, component := range strings.Split(target, "/") {
		switch component {
		case "", ".":
		case "..":
			if named {
				return errors.New("image symlink target has ambiguous internal parent traversal")
			}
		default:
			named = true
		}
	}
	return nil
}
