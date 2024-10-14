/*
 * mycfuse.go
 *
 * Copyright 2017-2022 Bill Zissimopoulos
 */
/*
 * This file is part of Cgofuse.
 *
 * It is licensed under the MIT license. The full license text can be found
 * in the License.txt file at the root of this project.
 */

package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/winfsp/cgofuse/examples/shared"
	"github.com/winfsp/cgofuse/fuse"
)

func trace(vals ...interface{}) func(vals ...interface{}) {
	uid, gid, _ := fuse.Getcontext()
	return shared.Trace(1, fmt.Sprintf("[uid=%v,gid=%v]", uid, gid), vals...)
}

func errno(err error) int {
	if nil != err {
		return -int(err.(syscall.Errno))
	} else {
		return 0
	}
}

var (
	_host *fuse.FileSystemHost
)

type DataStoreFuse struct {
	fuse.FileSystemBase
	root       string
	unixSocket net.Conn
	localFS    *LocalFS
}

func (ds *DataStoreFuse) Init() {
	defer trace()()
	e := syscall.Chdir(ds.root)
	if nil == e {
		ds.root = "./"
	}
}

func (ds *DataStoreFuse) Statfs(path string, stat *fuse.Statfs_t) (errc int) {
	defer trace(path)(&errc, stat)
	path = filepath.Join(ds.root, path)
	stgo := syscall.Statfs_t{}
	errc = errno(syscall_Statfs(path, &stgo))
	copyFusestatfsFromGostatfs(stat, &stgo)
	return
}

func (ds *DataStoreFuse) Mknod(path string, mode uint32, dev uint64) (errc int) {
	defer trace(path, mode, dev)(&errc)
	defer setuidgid()()
	path = filepath.Join(ds.root, path)
	return errno(syscall.Mknod(path, mode, int(dev)))
}

func (ds *DataStoreFuse) Mkdir(path string, mode uint32) (errc int) {
	defer trace(path, mode)(&errc)
	defer setuidgid()()
	path = filepath.Join(ds.root, path)
	return errno(syscall.Mkdir(path, mode))
}

func (ds *DataStoreFuse) Unlink(path string) (errc int) {
	defer trace(path)(&errc)
	path = filepath.Join(ds.root, path)
	return errno(syscall.Unlink(path))
}

func (ds *DataStoreFuse) Rmdir(path string) (errc int) {
	defer trace(path)(&errc)
	path = filepath.Join(ds.root, path)
	return errno(syscall.Rmdir(path))
}

func (ds *DataStoreFuse) Link(oldpath string, newpath string) (errc int) {
	defer trace(oldpath, newpath)(&errc)
	defer setuidgid()()
	oldpath = filepath.Join(ds.root, oldpath)
	newpath = filepath.Join(ds.root, newpath)
	return errno(syscall.Link(oldpath, newpath))
}

func (ds *DataStoreFuse) Symlink(target string, newpath string) (errc int) {
	defer trace(target, newpath)(&errc)
	defer setuidgid()()
	newpath = filepath.Join(ds.root, newpath)
	return errno(syscall.Symlink(target, newpath))
}

func (ds *DataStoreFuse) Readlink(path string) (errc int, target string) {
	defer trace(path)(&errc, &target)
	path = filepath.Join(ds.root, path)
	buff := [1024]byte{}
	n, e := syscall.Readlink(path, buff[:])
	if nil != e {
		return errno(e), ""
	}
	return 0, string(buff[:n])
}

func (ds *DataStoreFuse) Rename(oldpath string, newpath string) (errc int) {
	defer trace(oldpath, newpath)(&errc)
	defer setuidgid()()
	oldpath = filepath.Join(ds.root, oldpath)
	newpath = filepath.Join(ds.root, newpath)
	return errno(syscall.Rename(oldpath, newpath))
}

func (ds *DataStoreFuse) Chmod(path string, mode uint32) (errc int) {
	defer trace(path, mode)(&errc)
	path = filepath.Join(ds.root, path)
	return errno(syscall.Chmod(path, mode))
}

func (ds *DataStoreFuse) Chown(path string, uid uint32, gid uint32) (errc int) {
	defer trace(path, uid, gid)(&errc)
	path = filepath.Join(ds.root, path)
	return errno(syscall.Lchown(path, int(uid), int(gid)))
}

func (ds *DataStoreFuse) Utimens(path string, tmsp1 []fuse.Timespec) (errc int) {
	defer trace(path, tmsp1)(&errc)
	path = filepath.Join(ds.root, path)
	tmsp := [2]syscall.Timespec{}
	tmsp[0].Sec, tmsp[0].Nsec = tmsp1[0].Sec, tmsp1[0].Nsec
	tmsp[1].Sec, tmsp[1].Nsec = tmsp1[1].Sec, tmsp1[1].Nsec
	return errno(syscall.UtimesNano(path, tmsp[:]))
}

func (ds *DataStoreFuse) Create(path string, flags int, mode uint32) (errc int, fh uint64) {
	defer trace(path, flags, mode)(&errc, &fh)
	defer setuidgid()()
	return ds.open(path, flags, mode)
}

func (ds *DataStoreFuse) Open(path string, flags int) (errc int, fh uint64) {
	defer trace(path, flags)(&errc, &fh)
	return ds.open(path, flags, 0)
}

func (ds *DataStoreFuse) open(path string, flags int, mode uint32) (errc int, fh uint64) {
	path = filepath.Join(ds.root, path)
	f, e := syscall.Open(path, flags, mode)
	if nil != e {
		return errno(e), ^uint64(0)
	}
	return 0, uint64(f)
}

func (ds *DataStoreFuse) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	defer trace(path, fh)(&errc, stat)
	stgo := syscall.Stat_t{}
	if ^uint64(0) == fh {
		path = filepath.Join(ds.root, path)
		errc = errno(syscall.Lstat(path, &stgo))
	} else {
		errc = errno(syscall.Fstat(int(fh), &stgo))
	}
	copyFusestatFromGostat(stat, &stgo)
	return
}

func (ds *DataStoreFuse) Truncate(path string, size int64, fh uint64) (errc int) {
	defer trace(path, size, fh)(&errc)
	if ^uint64(0) == fh {
		path = filepath.Join(ds.root, path)
		errc = errno(syscall.Truncate(path, size))
	} else {
		errc = errno(syscall.Ftruncate(int(fh), size))
	}
	return
}

func (ds *DataStoreFuse) Read(path string, buff []byte, ofst int64, fh uint64) (n int) {
	defer trace(path, buff, ofst, fh)(&n)
	if strings.Contains(path, "-flat") {
		n, err := ds.localFS.ReadFile(path, buff, ofst)
		if err != nil {
			fmt.Println("error reading flat file", err)
			return errno(err)
		}
		fmt.Println("Read call on SocketPath", path, ofst, len(buff))
		return n
	} else {
		n, e := syscall.Pread(int(fh), buff, ofst)
		if nil != e {
			return errno(e)
		}
		return n
	}
}

func (ds *DataStoreFuse) ReadFromStorage(path string, buf []byte, offset int64) (n int) {
	// TODO Socket call here
	f, err := os.OpenFile("/root/disk-flat.vmdk", os.O_RDONLY, 0644)
	if err != nil {
		fmt.Println("error opening file", err)
		return errno(err)
	}
	defer func() {
		f.Close()
	}()
	fmt.Println("ReadFromStorage SocketPath", "/root/disk-flat.vmdk")
	n, err = f.ReadAt(buf, offset)
	if err != nil {
		fmt.Println("error reading file", err)
		return errno(err)
	}
	return n
}
func (ds *DataStoreFuse) Write(path string, buff []byte, ofst int64, fh uint64) (n int) {
	defer trace(path, buff, ofst, fh)(&n)
	fmt.Println("Write received for file", path, " offset", ofst)
	if strings.Contains(path, "-flat") {
		n, err := ds.localFS.WriteFile(path, buff, ofst)
		if err != nil {
			fmt.Println("Error writing flat file", err)
			return errno(err)
		}
		return n
	}
	n, e := syscall.Pwrite(int(fh), buff, ofst)
	if nil != e {
		return errno(e)
	}
	ds.Flush(path, fh)
	return n
}

func (ds *DataStoreFuse) Release(path string, fh uint64) (errc int) {
	defer trace(path, fh)(&errc)
	return errno(syscall.Close(int(fh)))
}

func (ds *DataStoreFuse) Fsync(path string, datasync bool, fh uint64) (errc int) {
	defer trace(path, datasync, fh)(&errc)
	return errno(syscall.Fsync(int(fh)))
}

func (ds *DataStoreFuse) Opendir(path string) (errc int, fh uint64) {
	defer trace(path)(&errc, &fh)
	path = filepath.Join(ds.root, path)
	f, e := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
	if nil != e {
		return errno(e), ^uint64(0)
	}
	return 0, uint64(f)
}

func (ds *DataStoreFuse) Readdir(path string,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool,
	ofst int64,
	fh uint64) (errc int) {
	defer trace(path, fill, ofst, fh)(&errc)
	path = filepath.Join(ds.root, path)
	file, e := os.Open(path)
	if nil != e {
		return errno(e)
	}
	defer file.Close()
	nams, e := file.Readdirnames(0)
	if nil != e {
		return errno(e)
	}
	nams = append([]string{".", ".."}, nams...)
	for _, name := range nams {
		if !fill(name, nil, 0) {
			break
		}
	}
	return 0
}

func (ds *DataStoreFuse) Releasedir(path string, fh uint64) (errc int) {
	defer trace(path, fh)(&errc)
	return errno(syscall.Close(int(fh)))
}

func (ds *DataStoreFuse) ConnectSocket(reqID string) {
	socket := "/tmp/flr_" + reqID + "_server"
	connection, err := net.Dial("unix", socket)
	if err != nil {
		panic("error connecting to socket")
	}

	ds.unixSocket = connection
}

func (ds *DataStoreFuse) CreateWriteFS() {
	localFS := GetLocalFSObject(ds.root, 1024*1024, ds)
	ds.localFS = localFS
}

func main() {
	reqID := os.Args[1]
	nfsSource := os.Args[2]
	mountPoint := os.Args[3]
	cFuseLogFile := os.Args[4]
	fmt.Println("ReqID", reqID, "nfsSource", nfsSource, "mountPoint", mountPoint, "log", cFuseLogFile)
	syscall.Umask(0)
	ptfs := DataStoreFuse{}
	//ptfs.ConnectSocket(reqID)

	_, err := os.Stat(nfsSource)
	if err != nil {
		panic("cannot stat nfs source path")
	}
	ptfs.root = nfsSource
	ptfs.CreateWriteFS()
	// TODO truncated flat files can be created here or at first read when stat fails to find the file.

	_host = fuse.NewFileSystemHost(&ptfs)
	_host.Mount(mountPoint, nil)
}
