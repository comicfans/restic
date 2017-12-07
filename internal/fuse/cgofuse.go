// +build windows

package fuse

import (
	"encoding/binary"
	"fmt"
	"github.com/billziss-gh/cgofuse/examples/shared"
	"github.com/billziss-gh/cgofuse/fuse"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/net/context"
	"hash/fnv"
	"strings"
	"sync"
)

func GenerateDynamicInode(parent uint64, name string) uint64 {
	h := fnv.New64a()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], parent)
	_, _ = h.Write(buf[:])
	_, _ = h.Write([]byte(name))
	var inode uint64
	for {
		inode = h.Sum64()
		if inode != 0 {
			break
		}
		// there's a tiny probability that result is zero; change the
		// input a little and try again
		_, _ = h.Write([]byte{'x'})
	}
	return inode
}

func (self *Cgofs) synchronize() func() {
	self.lock.Lock()
	return func() {
		self.lock.Unlock()
	}
}

type Node interface {
	GetInode() uint64
}

type DirNode interface {
	GetInode() uint64
	Readdir(ctx context.Context, path string,
		fill func(name string, stat *fuse.Stat_t, ofst int64) bool,
		ofst int64,
		fh uint64) (errc int)
}

// MetaDir is a fuse directory which contains other directories.
type MetaDir struct {
	inode   uint64
	root    *Root
	entries map[string]Node
}

func (d *MetaDir) GetInode() uint64 {
	return d.inode
}

func (f *file) GetInode() uint64 {
	return f.inode
}

type Cgofs struct {
	fuse.FileSystemBase
	lock    sync.Mutex
	root    *Root
	ctx     context.Context
	openmap map[uint64]Node
}

func NewCgofs(ctx context.Context, root *Root) *Cgofs {
	ret := &Cgofs{
		ctx:     ctx,
		root:    root,
		openmap: make(map[uint64]Node),
	}

	ret.openmap[root.inode] = root

	for _, v := range root.entries {
		ret.openmap[v.GetInode()] = v
	}

	return ret
}

func split(path string) []string {
	return strings.Split(path, "/")
}

func (self *Cgofs) lookupNode(path string, ancestor Node) (prnt Node, name string, node Node) {
	prnt = self.root
	name = ""
	node = self.root
	for _, c := range split(path) {
		if "" != c {
			if 255 < len(c) {
				panic(fuse.Error(-fuse.ENAMETOOLONG))
			}
			prnt, name = node, c

			metaDir, ok := node.(*MetaDir)
			if ok {
				//this node is a dir
				node = metaDir.entries[c]
				if nil != ancestor && node == ancestor {
					name = "" // special case loop condition
					return
				}
			} else {
				//this node is a file
				//TODO
				return
			}
		}
	}
	return
}
func (self *Cgofs) openNode(path string, dir bool) (int, uint64) {
	_, _, node := self.lookupNode(path, nil)
	if nil == node {
		return -fuse.ENOENT, ^uint64(0)
	}

	_, node_is_dir := node.(DirNode)

	if dir != node_is_dir {
		return -fuse.ENOTDIR, ^uint64(0)
	}

	return 0, node.GetInode()
}

func (self *Cgofs) Opendir(path string) (errc int, fh uint64) {
	defer trace(path)(&errc, &fh)
	return self.openNode(path, true)
}

func trace(vals ...interface{}) func(vals ...interface{}) {
	uid, gid, _ := fuse.Getcontext()
	return shared.Trace(1, fmt.Sprintf("[uid=%v,gid=%v]", uid, gid), vals...)
}

func NewMetaDir(root *Root, inode uint64, entries map[string]Node) *MetaDir {
	debug.Log("new meta dir with %d entries, inode %d", len(entries), inode)

	return &MetaDir{
		root:    root,
		inode:   inode,
		entries: entries,
	}
}

func (self *Cgofs) Releasedir(path string, fh uint64) (errc int) {
	defer trace(path, fh)(&errc)
	defer self.synchronize()()
	return 0
}

// NewRoot initializes a new root node from a repository.
func NewRoot(ctx context.Context, repo restic.Repository, cfg Config) (*Root, error) {
	debug.Log("NewRoot(), config %v", cfg)

	root := &Root{
		repo:          repo,
		inode:         rootInode,
		cfg:           cfg,
		blobSizeCache: NewBlobSizeCache(ctx, repo.Index()),
	}

	entries := map[string]Node{
		"snapshots": NewSnapshotsDir(root, GenerateDynamicInode(root.inode, "snapshots"), "", ""),
		"tags":      NewTagsDir(root, GenerateDynamicInode(root.inode, "tags")),
		"hosts":     NewHostsDir(root, GenerateDynamicInode(root.inode, "hosts")),
		"ids":       NewSnapshotsIDSDir(root, GenerateDynamicInode(root.inode, "ids")),
	}

	root.MetaDir = NewMetaDir(root, rootInode, entries)

	return root, nil
}

func (self *Cgofs) Readdir(
	path string,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool,
	ofst int64,
	fh uint64) (errc int) {
	defer trace(path, fill, ofst, fh)(&errc)
	defer self.synchronize()()
	dirNode := self.openmap[fh].(DirNode)

	return dirNode.Readdir(self.ctx, path, fill, ofst, fh)
}

func (d *dir) Readdir(
	ctx context.Context,
	path string,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool,
	ofst int64,
	fh uint64) (errc int) {
	defer trace(path, fill, ofst, fh)(&errc)

	uid, gid, _ := fuse.Getcontext()
	fill(".", &fuse.Stat_t{
		Ino:   d.inode,
		Nlink: 1,
		Uid:   uid,
		Gid:   gid,
		Flags: 0,
	}, 0)

	fill("..", nil, 0)

	for _, node := range d.items {

		stat := fuse.Stat_t{
			Ino:   GenerateDynamicInode(d.inode, node.Name),
			Nlink: 1,
			Uid:   uid,
			Gid:   gid,
			Flags: 0,
		}

		switch node.Type {
		case "dir":
			stat.Mode = fuse.S_IFDIR | 00777
		case "file":
			stat.Mode = fuse.S_IFREG | 00777
		case "symlink":
			stat.Mode = fuse.S_IFLNK | 00777
		}

		fill(node.Name, &stat, 0)

	}

	return 0
}

func (self *Cgofs) getNode(path string, fh uint64) Node {
	if ^uint64(0) == fh {
		_, _, node := self.lookupNode(path, nil)
		return node
	} else {
		return self.openmap[fh]
	}
}

func (self *Cgofs) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	defer trace(path, fh)(&errc, stat)
	defer self.synchronize()()
	node := self.getNode(path, fh)
	if nil == node {
		return -fuse.ENOENT
	}

	uid, gid, _ := fuse.Getcontext()
	_, is_dir := node.(DirNode)

	stat.Uid = uid
	stat.Gid = gid
	stat.Nlink = 1
	stat.Flags = 0
	if is_dir {
		stat.Mode = fuse.S_IFDIR | 00777
	} else {
		stat.Mode = fuse.S_IFREG | 00777
	}

	return 0
}

func (d *SnapshotsDir) GetInode() uint64 {
	return d.inode
}

func (d *SnapshotsDir) Readdir(
	ctx context.Context,
	path string,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool,
	ofst int64,
	fh uint64) (errc int) {

	// update snapshots
	updateSnapshots(ctx, d.root)

	// update snapshot names
	updateSnapshotNames(d)

	defer trace(path, fill, ofst, fh)(&errc)

	fill(".", &fuse.Stat_t{
		Ino:   d.inode,
		Uid:   0,
		Gid:   0,
		Flags: 0,
	}, 0)
	fill("..", nil, 0)

	for name := range d.names {

		stat := fuse.Stat_t{
			Mode:  fuse.S_IFDIR | 00777,
			Ino:   GenerateDynamicInode(d.inode, name),
			Nlink: 1,
			Uid:   0,
			Gid:   gid,
			Flags: 0,
		}

		fill(name, &stat, 0)
	}

	// Latest
	if d.latest != "" {
		fill("latest", &fuse.Stat_t{
			Mode:  fuse.S_IFDIR | 00777,
			Ino:   GenerateDynamicInode(d.inode, "latest"),
			Nlink: 1,
			Uid:   0,
			Gid:   gid,
			Flags: 0,
		}, 0)
	}

	return 0
}

func (d *SnapshotsIDSDir) GetInode() uint64 {
	return d.inode
}

// ReadDirAll returns all entries of the SnapshotsIDSDir.
func (d *SnapshotsIDSDir) Readdir(

	ctx context.Context,
	path string,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool,
	ofst int64,
	fh uint64) (errc int) {

	// update snapshots
	updateSnapshots(ctx, d.root)

	// update snapshot ids
	updateSnapshotIDSNames(d)

	defer trace(path, fill, ofst, fh)(&errc)

	fill(".", &fuse.Stat_t{
		Mode:  fuse.S_IFDIR | 00777,
		Ino:   d.inode,
		Uid:   0,
		Gid:   0,
		Flags: 0,
		Nlink: 1,
	}, 0)
	fill("..", nil, 0)

	for name := range d.names {
		fill(name, &fuse.Stat_t{
			Mode:  fuse.S_IFDIR | 00777,
			Ino:   GenerateDynamicInode(d.inode, name),
			Nlink: 1,
			Uid:   uid,
			Gid:   gid,
			Flags: 0,
		}, 0)
	}

	return 0
}

func (d *HostsDir) GetInode() uint64 {
	return d.inode
}

// ReadDirAll returns all entries of the HostsDir.
func (d *HostsDir) Readdir(path string,

	ctx context.Context,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool,
	ofst int64,
	fh uint64) (errc int) {

	// update snapshots
	updateSnapshots(ctx, d.root)

	// update host names
	updateHostsNames(d)

	defer trace(path, fill, ofst, fh)(&errc)

	fill(".", &fuse.Stat_t{
		Ino:   d.inode,
		Mode:  fuse.S_IFDIR | 00777,
		Uid:   0,
		Gid:   0,
		Flags: 0,
	}, 0)

	fill("..", nil, 0)

	for host := range d.hosts {
		fill(host, &fuse.Stat_t{
			Ino:   GenerateDynamicInode(d.inode, host),
			Mode:  fuse.S_IFDIR | 00777,
			Uid:   0,
			Gid:   0,
			Flags: 0,
		}, 0)
	}

	return 0
}

func (d *TagsDir) GetInode() uint64 {
	return d.inode
}

func defaultDir(ino uint64) *fuse.Stat_t {
	tmsp := fuse.Now()

	return fuse.Stat_t{
		Ino:      ino,
		Uid:      0,
		Gid:      0,
		Nlink:    1,
		Uid:      0,
		Gid:      0,
		Mode:     fuse.S_IFDIR | 00777,
		Atim:     tmsp,
		Mtim:     tmsp,
		Ctim:     tmsp,
		Birthtim: tmsp,
		Flags:    0,
	}
}

// ReadDirAll returns all entries of the TagsDir.
func (d *TagsDir) Readdir(path string,

	ctx context.Context,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool,
	ofst int64,
	fh uint64) (errc int) {

	defer trace(path, fill, ofst, fh)(&errc)

	// update snapshots
	updateSnapshots(ctx, d.root)

	// update tag names
	updateTagNames(d)

	fill(".", defaultDir(d.inode), 0)
	fill("..", nil, 0)

	for tag := range d.tags {

		fill(tag, defaultDir(GenerateDynamicInode(d.inode, tag)), 0)

	}

	return 0
}
