// +build windows

package main

import (
	"context"

	"github.com/spf13/cobra"

	//	"github.com/billziss-gh/cgofuse/examples/memfs"
	cgofuse "github.com/billziss-gh/cgofuse/fuse"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/fuse"
	"github.com/restic/restic/internal/restic"
)

var cmdMount = &cobra.Command{
	Use:   "mount [flags] mountpoint",
	Short: "Mount the repository",
	Long: `
The "mount" command mounts the repository via fuse to a directory. This is a
read-only mount.
`,
	DisableAutoGenTag: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runMount(mountOptions, globalOptions, args)
	},
}

var host *cgofuse.FileSystemHost

// MountOptions collects all options for the mount command.
type MountOptions struct {
	OwnerRoot  bool
	AllowRoot  bool
	AllowOther bool
	Host       string
	Tags       restic.TagLists
	Paths      []string
}

var mountOptions MountOptions

func init() {
	cmdRoot.AddCommand(cmdMount)

	mountFlags := cmdMount.Flags()
	mountFlags.BoolVar(&mountOptions.OwnerRoot, "owner-root", false, "use 'root' as the owner of files and dirs")
	mountFlags.BoolVar(&mountOptions.AllowRoot, "allow-root", false, "allow root user to access the data in the mounted directory")
	mountFlags.BoolVar(&mountOptions.AllowOther, "allow-other", false, "allow other users to access the data in the mounted directory")

	mountFlags.StringVarP(&mountOptions.Host, "host", "H", "", `only consider snapshots for this host`)
	mountFlags.Var(&mountOptions.Tags, "tag", "only consider snapshots which include this `taglist`")
	mountFlags.StringArrayVar(&mountOptions.Paths, "path", nil, "only consider snapshots which include this (absolute) `path`")
}

func mount(opts MountOptions, gopts GlobalOptions, mountpoint string) error {
	debug.Log("start mount")
	defer debug.Log("finish mount")

	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}

	lock, err := lockRepo(repo)
	defer unlockRepo(lock)
	if err != nil {
		return err
	}

	err = repo.LoadIndex(context.TODO())
	if err != nil {
		return err
	}

	mountOptions := []string{
	//	"ro",
	//	"fsname=restic",
	}

	if opts.AllowRoot {
		mountOptions = append(mountOptions, "allow_root")
	}

	if opts.AllowOther {
		mountOptions = append(mountOptions, "allow_other")
	}

	cfg := fuse.Config{
		OwnerIsRoot: opts.OwnerRoot,
		Host:        opts.Host,
		Tags:        opts.Tags,
		Paths:       opts.Paths,
	}

	root, err := fuse.NewRoot(context.TODO(), repo, cfg)
	if err != nil {
		return err
	}
	fs := fuse.NewCgofs(context.TODO(), root)

	Printf("Now serving the repository at %s\n", mountpoint)
	Printf("Don't forget to umount after quitting!\n")

	debug.Log("serving mount at %v", mountpoint)

	/*
		memfs := memfs.NewMemfs()
		host := cgofuse.NewFileSystemHost(memfs)
		host.SetCapReaddirPlus(true)
		host.Mount("z:", nil)

	*/
	host = cgofuse.NewFileSystemHost(fs)
	host.Mount("Z:", nil)

	return nil
}

func umount(mountpoint string) error {
	//	host.Unmount()
	return nil
}

func runMount(opts MountOptions, gopts GlobalOptions, args []string) error {
	if len(args) == 0 {
		return errors.Fatal("wrong number of parameters")
	}

	mountpoint := args[0]

	AddCleanupHandler(func() error {
		debug.Log("running umount cleanup handler for mount at %v", mountpoint)
		err := umount(mountpoint)
		if err != nil {
			Warnf("unable to umount (maybe already umounted?): %v\n", err)
		}
		return nil
	})

	return mount(opts, gopts, mountpoint)
}
