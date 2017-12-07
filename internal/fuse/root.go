// +build !openbsd
// +build !windows

package fuse

import (
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/restic"

	"golang.org/x/net/context"

	"bazil.org/fuse/fs"
)

// ensure that *Root implements these interfaces
var _ = fs.HandleReadDirAller(&Root{})
var _ = fs.NodeStringLookuper(&Root{})

// NewRoot initializes a new root node from a repository.
func NewRoot(ctx context.Context, repo restic.Repository, cfg Config) (*Root, error) {
	debug.Log("NewRoot(), config %v", cfg)

	root := &Root{
		repo:          repo,
		inode:         rootInode,
		cfg:           cfg,
		blobSizeCache: NewBlobSizeCache(ctx, repo.Index()),
	}

	entries := map[string]fs.Node{
		"snapshots": NewSnapshotsDir(root, fs.GenerateDynamicInode(root.inode, "snapshots"), "", ""),
		"tags":      NewTagsDir(root, fs.GenerateDynamicInode(root.inode, "tags")),
		"hosts":     NewHostsDir(root, fs.GenerateDynamicInode(root.inode, "hosts")),
		"ids":       NewSnapshotsIDSDir(root, fs.GenerateDynamicInode(root.inode, "ids")),
	}

	root.MetaDir = NewMetaDir(root, rootInode, entries)

	return root, nil
}

// Root is just there to satisfy fs.Root, it returns itself.
func (r *Root) Root() (fs.Node, error) {
	debug.Log("Root()")
	return r, nil
}
