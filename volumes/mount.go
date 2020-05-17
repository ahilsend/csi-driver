package volumes

import (
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"path/filepath"

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
	PublishFilesystem(volume *csi.Volume, targetPath string, stagingTargetPath string, opts MountOpts) error
	PublishBlock(volume *csi.Volume, targetPath string, opts MountOpts) error
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
			if err := s.makeDir(stagingTargetPath); err != nil {
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

func (s *LinuxMountService) PublishFilesystem(volume *csi.Volume, targetPath string, stagingTargetPath string, opts MountOpts) error {
	level.Debug(s.logger).Log(
		"msg", "publishing fs volume",
		"volume-name", volume.Name,
		"target-path", targetPath,
		"staging-target-path", stagingTargetPath,
		"fs-type", opts.FSType,
		"readonly", opts.Readonly,
		"additional-mount-options", opts.Additional,
	)

	if err := s.makeDir(targetPath); err != nil {
		return err
	}
	return s.mountBind(stagingTargetPath, targetPath, opts)
}

func (s *LinuxMountService) PublishBlock(volume *csi.Volume, targetPath string, opts MountOpts) error {
	level.Debug(s.logger).Log(
		"msg", "publishing block volume",
		"volume-name", volume.Name,
		"target-path", targetPath,
		"volume-path", volume.LinuxDevice,
		"readonly", opts.Readonly,
		"additional-mount-options", opts.Additional,
	)

	targetDir := filepath.Dir(targetPath)

	// create the global mount path if it is missing
	// Path in the form of /var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/publish/{volumeName}
	exists, err := s.PathExists(targetDir)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to check if path exists %q: %v", targetDir, err)
	}

	if !exists {
		if err := s.makeDir(targetDir); err != nil {
			return err
		}
	}

	// Create the mount point as a file since bind mount device node requires it to be a file
	if err := s.makeFile(targetPath); err != nil {
		if removeErr := os.Remove(targetPath); removeErr != nil {
			return status.Errorf(codes.Internal, "failed to remove mount target %q: %v", targetPath, removeErr)
		}
		return status.Errorf(codes.Internal, "failed to create block mount file %q: %v", targetPath, err)
	}

	return s.mountBind(volume.LinuxDevice, targetPath, opts)
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

func (s *LinuxMountService) makeFile(pathname string) error {
	f, err := os.OpenFile(pathname, os.O_CREATE, os.FileMode(0644))
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	defer f.Close()
	return nil
}

func (s *LinuxMountService) makeDir(pathname string) error {
	err := os.MkdirAll(pathname, os.FileMode(0755))
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	return nil
}

func (s *LinuxMountService) mountBind(sourcePath string, targetPath string, opts MountOpts) error {

	options := []string{"bind"}
	if opts.Readonly {
		options = append(options, "ro")
	}
	options = append(options, opts.Additional...)

	return s.mounter.Interface.Mount(sourcePath, targetPath, opts.FSType, options)
}
