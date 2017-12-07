// +build windows

package fuse

import (
	"fmt"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/net/context"
	"os"
	"time"
)

// Config holds settings for the fuse mount.
type Config struct {
	OwnerIsRoot bool
	Host        string
	Tags        []restic.TagList
	Paths       []string
}

// search element in string list.
func isElem(e string, list []string) bool {
	for _, x := range list {
		if e == x {
			return true
		}
	}
	return false
}

const rootInode = 1

// Root is the root node of the fuse mount of a repository.
type Root struct {
	repo          restic.Repository
	cfg           Config
	inode         uint64
	snapshots     restic.Snapshots
	blobSizeCache *BlobSizeCache
	snCount       int

	*MetaDir
}

// SnapshotsDir is a fuse directory which contains snapshots named by timestamp.
type SnapshotsDir struct {
	inode   uint64
	root    *Root
	names   map[string]*restic.Snapshot
	latest  string
	tag     string
	host    string
	snCount int
}

// SnapshotsIDSDir is a fuse directory which contains snapshots named by ids.
type SnapshotsIDSDir struct {
	inode   uint64
	root    *Root
	names   map[string]*restic.Snapshot
	snCount int
}

// HostsDir is a fuse directory which contains hosts.
type HostsDir struct {
	inode   uint64
	root    *Root
	hosts   map[string]bool
	snCount int
}

// TagsDir is a fuse directory which contains tags.
type TagsDir struct {
	inode   uint64
	root    *Root
	tags    map[string]bool
	snCount int
}

// SnapshotLink
type snapshotLink struct {
	root     *Root
	inode    uint64
	target   string
	snapshot *restic.Snapshot
}

// read tag names from the current repository-state.
func updateTagNames(d *TagsDir) {
	if d.snCount != d.root.snCount {
		d.snCount = d.root.snCount
		d.tags = make(map[string]bool, len(d.root.snapshots))
		for _, snapshot := range d.root.snapshots {
			for _, tag := range snapshot.Tags {
				if tag != "" {
					d.tags[tag] = true
				}
			}
		}
	}
}

// read host names from the current repository-state.
func updateHostsNames(d *HostsDir) {
	if d.snCount != d.root.snCount {
		d.snCount = d.root.snCount
		d.hosts = make(map[string]bool, len(d.root.snapshots))
		for _, snapshot := range d.root.snapshots {
			d.hosts[snapshot.Hostname] = true
		}
	}
}

// read snapshot id names from the current repository-state.
func updateSnapshotIDSNames(d *SnapshotsIDSDir) {
	if d.snCount != d.root.snCount {
		d.snCount = d.root.snCount
		for _, sn := range d.root.snapshots {
			name := sn.ID().Str()
			d.names[name] = sn
		}
	}
}

// NewSnapshotsDir returns a new directory containing snapshots.
func NewSnapshotsDir(root *Root, inode uint64, tag string, host string) *SnapshotsDir {
	debug.Log("create snapshots dir, inode %d", inode)
	d := &SnapshotsDir{
		root:   root,
		inode:  inode,
		names:  make(map[string]*restic.Snapshot),
		latest: "",
		tag:    tag,
		host:   host,
	}

	return d
}

// NewSnapshotsIDSDir returns a new directory containing snapshots named by ids.
func NewSnapshotsIDSDir(root *Root, inode uint64) *SnapshotsIDSDir {
	debug.Log("create snapshots ids dir, inode %d", inode)
	d := &SnapshotsIDSDir{
		root:  root,
		inode: inode,
		names: make(map[string]*restic.Snapshot),
	}

	return d
}

// NewHostsDir returns a new directory containing host names
func NewHostsDir(root *Root, inode uint64) *HostsDir {
	debug.Log("create hosts dir, inode %d", inode)
	d := &HostsDir{
		root:  root,
		inode: inode,
		hosts: make(map[string]bool),
	}

	return d
}

// NewTagsDir returns a new directory containing tag names
func NewTagsDir(root *Root, inode uint64) *TagsDir {
	debug.Log("create tags dir, inode %d", inode)
	d := &TagsDir{
		root:  root,
		inode: inode,
		tags:  make(map[string]bool),
	}

	return d
}

// update snapshots if repository has changed
func updateSnapshots(ctx context.Context, root *Root) {
	snapshots := restic.FindFilteredSnapshots(ctx, root.repo, root.cfg.Host, root.cfg.Tags, root.cfg.Paths)
	if root.snCount != len(snapshots) {
		root.snCount = len(snapshots)
		root.repo.LoadIndex(ctx)
		root.snapshots = snapshots
	}
}

// read snapshot timestamps from the current repository-state.
func updateSnapshotNames(d *SnapshotsDir) {
	if d.snCount != d.root.snCount {
		d.snCount = d.root.snCount
		var latestTime time.Time
		d.latest = ""
		d.names = make(map[string]*restic.Snapshot, len(d.root.snapshots))
		for _, sn := range d.root.snapshots {
			if d.tag == "" || isElem(d.tag, sn.Tags) {
				if d.host == "" || d.host == sn.Hostname {
					name := sn.Time.Format(time.RFC3339)
					if d.latest == "" || !sn.Time.Before(latestTime) {
						latestTime = sn.Time
						d.latest = name
					}
					for i := 1; ; i++ {
						if _, ok := d.names[name]; !ok {
							break
						}

						name = fmt.Sprintf("%s-%d", sn.Time.Format(time.RFC3339), i)
					}

					d.names[name] = sn
				}
			}
		}
	}
}

type dir struct {
	root        *Root
	items       map[string]*restic.Node
	inode       uint64
	parentInode uint64
	node        *restic.Node

	blobsize *BlobSizeCache
}

func newDir(ctx context.Context, root *Root, inode, parentInode uint64, node *restic.Node) (*dir, error) {
	debug.Log("new dir for %v (%v)", node.Name, node.Subtree.Str())
	tree, err := root.repo.LoadTree(ctx, *node.Subtree)
	if err != nil {
		debug.Log("  error loading tree %v: %v", node.Subtree.Str(), err)
		return nil, err
	}
	items := make(map[string]*restic.Node)
	for _, node := range tree.Nodes {
		items[node.Name] = node
	}

	return &dir{
		root:        root,
		node:        node,
		items:       items,
		inode:       inode,
		parentInode: parentInode,
	}, nil
}

// replaceSpecialNodes replaces nodes with name "." and "/" by their contents.
// Otherwise, the node is returned.
func replaceSpecialNodes(ctx context.Context, repo restic.Repository, node *restic.Node) ([]*restic.Node, error) {
	if node.Type != "dir" || node.Subtree == nil {
		return []*restic.Node{node}, nil
	}

	if node.Name != "." && node.Name != "/" {
		return []*restic.Node{node}, nil
	}

	tree, err := repo.LoadTree(ctx, *node.Subtree)
	if err != nil {
		return nil, err
	}

	return tree.Nodes, nil
}

func newDirFromSnapshot(ctx context.Context, root *Root, inode uint64, snapshot *restic.Snapshot) (*dir, error) {
	debug.Log("new dir for snapshot %v (%v)", snapshot.ID().Str(), snapshot.Tree.Str())
	tree, err := root.repo.LoadTree(ctx, *snapshot.Tree)
	if err != nil {
		debug.Log("  loadTree(%v) failed: %v", snapshot.ID().Str(), err)
		return nil, err
	}
	items := make(map[string]*restic.Node)
	for _, n := range tree.Nodes {
		nodes, err := replaceSpecialNodes(ctx, root.repo, n)
		if err != nil {
			debug.Log("  replaceSpecialNodes(%v) failed: %v", n, err)
			return nil, err
		}

		for _, node := range nodes {
			items[node.Name] = node
		}
	}

	return &dir{
		root: root,
		node: &restic.Node{
			UID:        uint32(os.Getuid()),
			GID:        uint32(os.Getgid()),
			AccessTime: snapshot.Time,
			ModTime:    snapshot.Time,
			ChangeTime: snapshot.Time,
			Mode:       os.ModeDir | 0555,
		},
		items: items,
		inode: inode,
	}, nil
}
func (d *dir) calcNumberOfLinks() uint32 {
	// a directory d has 2 hardlinks + the number
	// of directories contained by d
	var count uint32
	count = 2
	for _, node := range d.items {
		if node.Type == "dir" {
			count++
		}
	}
	return count
}

type file struct {
	root  *Root
	node  *restic.Node
	inode uint64

	sizes []int
	blobs [][]byte
}

func newFile(ctx context.Context, root *Root, inode uint64, node *restic.Node) (fusefile *file, err error) {
	debug.Log("create new file for %v with %d blobs", node.Name, len(node.Content))
	var bytes uint64
	sizes := make([]int, len(node.Content))
	for i, id := range node.Content {
		size, ok := root.blobSizeCache.Lookup(id)
		if !ok {
			size, err = root.repo.LookupBlobSize(id, restic.DataBlob)
			if err != nil {
				return nil, err
			}
		}

		sizes[i] = int(size)
		bytes += uint64(size)
	}

	if bytes != node.Size {
		debug.Log("sizes do not match: node.Size %v != size %v, using real size", node.Size, bytes)
		node.Size = bytes
	}

	return &file{
		inode: inode,
		root:  root,
		node:  node,
		sizes: sizes,
		blobs: make([][]byte, len(node.Content)),
	}, nil
}

func (f *file) getBlobAt(ctx context.Context, i int) (blob []byte, err error) {
	debug.Log("getBlobAt(%v, %v)", f.node.Name, i)
	if f.blobs[i] != nil {
		return f.blobs[i], nil
	}

	// release earlier blobs
	for j := 0; j < i; j++ {
		f.blobs[j] = nil
	}

	buf := restic.NewBlobBuffer(f.sizes[i])
	n, err := f.root.repo.LoadBlob(ctx, restic.DataBlob, f.node.Content[i], buf)
	if err != nil {
		debug.Log("LoadBlob(%v, %v) failed: %v", f.node.Name, f.node.Content[i], err)
		return nil, err
	}
	f.blobs[i] = buf[:n]

	return buf[:n], nil
}
