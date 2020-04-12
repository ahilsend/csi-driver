package volumes

import (
	"fmt"
	"os"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"k8s.io/utils/exec"
	"k8s.io/utils/mount"

	"hetzner.cloud/csi/csi"
)

const DefaultFSType = "ext4"

// MountOpts specifies options for mounting a volume.
type MountOpts struct {
	FSType     string
	Readonly   bool
	Additional []string // Additional mount options/flags passed to /bin/mount
}

func NewMountOpts() MountOpts {
	return MountOpts{
		FSType: DefaultFSType,
	}
}

// MountService mounts volumes.
type MountService interface {
	Stage(volume *csi.Volume, stagingTargetPath string, opts MountOpts) error
	Unstage(volume *csi.Volume, stagingTargetPath string) error
	Publish(volume *csi.Volume, targetPath string, stagingTargetPath string, opts MountOpts) error
	Unpublish(volume *csi.Volume, targetPath string) error
	PathExists(path string) (bool, error)
}

// LinuxMountService mounts volumes on a Linux system.
type LinuxMountService struct {
	logger  log.Logger
	mounter *mount.SafeFormatAndMount
}

func NewLinuxMountService(logger log.Logger) *LinuxMountService {
	return &LinuxMountService{
		logger: logger,
		mounter: &mount.SafeFormatAndMount{
			Interface: mount.New(""),
			Exec:      exec.New(),
		},
	}
}

func (s *LinuxMountService) Stage(volume *csi.Volume, stagingTargetPath string, opts MountOpts) error {
	level.Debug(s.logger).Log(
		"msg", "staging volume",
		"volume-name", volume.Name,
		"staging-target-path", stagingTargetPath,
		"fs-type", opts.FSType,
	)

	isNotMountPoint, err := s.mounter.Interface.IsLikelyNotMountPoint(stagingTargetPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := makeDir(stagingTargetPath); err != nil {
				return err
			}
			isNotMountPoint = true
		} else {
			return err
		}
	}
	if !isNotMountPoint {
		return fmt.Errorf("%q is not a valid mount point", stagingTargetPath)
	}

	return s.mounter.FormatAndMount(volume.LinuxDevice, stagingTargetPath, opts.FSType, nil)
}

func (s *LinuxMountService) Unstage(volume *csi.Volume, stagingTargetPath string) error {
	level.Debug(s.logger).Log(
		"msg", "unstaging volume",
		"volume-name", volume.Name,
		"staging-target-path", stagingTargetPath,
	)
	return s.mounter.Interface.Unmount(stagingTargetPath)
}

func (s *LinuxMountService) Publish(volume *csi.Volume, targetPath string, stagingTargetPath string, opts MountOpts) error {
	level.Debug(s.logger).Log(
		"msg", "publishing volume",
		"volume-name", volume.Name,
		"target-path", targetPath,
		"staging-target-path", stagingTargetPath,
		"fs-type", opts.FSType,
		"readonly", opts.Readonly,
		"additional-mount-options", opts.Additional,
	)

	if err := makeDir(targetPath); err != nil {
		return err
	}

	options := []string{"bind"}
	if opts.Readonly {
		options = append(options, "ro")
	}
	for _, o := range opts.Additional {
		options = append(options, o)
	}

	if err := s.mounter.Interface.Mount(stagingTargetPath, targetPath, opts.FSType, options); err != nil {
		return err
	}

	return nil
}

func (s *LinuxMountService) Unpublish(volume *csi.Volume, targetPath string) error {
	level.Debug(s.logger).Log(
		"msg", "unpublishing volume",
		"volume-name", volume.Name,
		"target-path", targetPath,
	)
	return s.mounter.Interface.Unmount(targetPath)
}

func (s *LinuxMountService) PathExists(path string) (bool, error) {
	level.Debug(s.logger).Log(
		"msg", "checking path existence",
		"path", path,
	)
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

/// makeDir creates a new directory.
// If pathname already exists as a directory, no error is returned.
// If pathname already exists as a file, an error is returned.
func makeDir(pathname string) error {
	err := os.MkdirAll(pathname, os.FileMode(0755))
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	return nil
}
