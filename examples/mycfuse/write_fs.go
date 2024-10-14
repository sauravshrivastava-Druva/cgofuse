package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
)

type Reader interface {
	ReadFromStorage(path string, buf []byte, offset int64) (n int)
}
type LocalFS struct {
	root      string
	blockSize int64
	blocks    map[int64]bool
	Reader
}

// GetLocalFSObject
func GetLocalFSObject(root string, blockSize int64, reader Reader) *LocalFS {
	var blocks = make(map[int64]bool, 0)

	return &LocalFS{
		root,
		blockSize,
		blocks,
		reader,
	}
}

// TODO validate if this is required
func (fs *LocalFS) Create(name string, fileSize int64) error {
	_, err := os.Stat(name)
	if err == nil {
		return nil
	}
	file, err := os.Create(name)
	// command line to create sparse file.
	//"dd if=/dev/zero of=sparse_file bs=1 count=0 seek=512M"
	if err != nil {
		return err
	}
	defer file.Close()

	if err = file.Truncate(fileSize); err != nil {
		return err
	}

	//TODO Revisit Fallocate with params
	// Set the file as sparse
	//if err = syscall.Fallocate(int(file.Fd()), syscall.FALLOC_FL_KEEP_SIZE|syscall.FALLOC_FL_PUNCH_HOLE, 0, fileSize); err != nil {
	//	return err
	//}
	//fs.fd, err = os.OpenFile(name, os.O_RDWR, 644)
	//if err != nil {
	//	return err
	//}
	return nil
}

func (fs *LocalFS) ReadFile(path string, buf []byte, offset int64) (n int, err error) {
	bufLength := int64(len(buf))
	blocks, err := NormaliseOffset(offset, bufLength, fs.blockSize)
	if err != nil {
		fmt.Println("error normalising the blocks", err)
		return 0, err
	}
	fd, err := os.OpenFile(fs.root+path, os.O_RDONLY, 644)
	if err != nil {
		fmt.Println("Error opening file when updating writes", path, err)
		return n, err
	}
	defer func() {
		fd.Close()
	}()
	fmt.Println("ReadFile SocketPath", fs.root+path)
	var readBuf []byte
	var readAlignedOffset int64
	for i, alignedOffset := range blocks {
		if i == 0 {
			readAlignedOffset = alignedOffset
		}
		storageBuf := make([]byte, fs.blockSize)
		if _, ok := fs.blocks[alignedOffset]; ok {
			fmt.Println("Cache hit", alignedOffset)
			_, err := fd.ReadAt(storageBuf, alignedOffset)
			if err != nil {
				fmt.Println("Error reading local file ", err)
				return n, err
			}
		} else {
			if fs.Reader.ReadFromStorage(path, storageBuf, alignedOffset) < 0 {
				fmt.Println("error reading file from storage")
				return n, errors.New("error reading file from storage")
			}
		}
		readBuf = append(readBuf, storageBuf...)
	}

	readIndex := offset - readAlignedOffset
	copy(buf, readBuf[readIndex:readIndex+bufLength])
	n = len(buf)
	originalBuf := make([]byte, bufLength)

	originalFD, err := os.OpenFile("/root/disk-flat.vmdk", os.O_RDONLY, 644)
	if err != nil {
		fmt.Println("Error opening file when updating writes", path, err)
		return n, err
	}
	defer originalFD.Close()
	_, err = originalFD.ReadAt(originalBuf, offset)
	if err != nil {
		fmt.Println("error reading original buff", err)
	}
	//originalMd5 := sha1.Sum(originalBuf)
	//trimmedMd5 := sha1.Sum(buf)
	if bytes.Equal(originalBuf[:], buf[:]) {
		fmt.Println("SocketPath", fs.root+path, "Offset", offset, "length", bufLength, "readAlignedOffset", readAlignedOffset, "ReadBuffLen", len(readBuf), "buflen", len(buf), "readIndex", readIndex)
	}

	return n, nil
}

func (fs *LocalFS) WriteFile(path string, buf []byte, offset int64) (n int, err error) {
	length := int64(len(buf))
	blocks, err := NormaliseOffset(offset, length, fs.blockSize)
	if err != nil {
		fmt.Println("Error normalising blocks")
		return n, err
	}
	fd, err := os.OpenFile(fs.root+path, os.O_RDWR, 644)
	if err != nil {
		fmt.Println("Error opening file when updating writes", path, err)
		return n, err
	}
	defer func() {
		fd.Close()
	}()
	fmt.Println("WriteFile SocketPath", fs.root+path)
	// writeBuf will be the buffer written on sparse disk.
	var writeBuf []byte
	// writeOffset is aligned block offset on which we will write blocks.
	var writeOffset int64

	for i, alignedOffset := range blocks {
		if i == 0 {
			writeOffset = alignedOffset
		}
		storageBuf := make([]byte, fs.blockSize)
		if _, ok := fs.blocks[alignedOffset]; ok {
			_, err := fd.ReadAt(storageBuf, alignedOffset)
			if err != nil {
				fmt.Println("Error reading local file ", err)
				return n, err
			}
		} else {
			if fs.Reader.ReadFromStorage(path, storageBuf, alignedOffset) < 0 {
				fmt.Println("error reading file from storage")
				return n, errors.New("error reading file from storage")
			}
		}
		writeBuf = append(writeBuf, storageBuf...)
		// TODO fs.blocks can be taken from sql-lite.
		fs.blocks[alignedOffset] = true
		fmt.Println("Adding aligned offset ", alignedOffset)
	}
	writeBuf = PatchBlock(writeBuf, buf, offset-writeOffset)
	n, err = fd.WriteAt(writeBuf, writeOffset)
	if err != nil {
		fmt.Println("Error writing to a file")
		return n, err
	}
	fmt.Println("Writing", fs.root+path, "offset", offset, "length", length, "writeOffset", writeOffset, "lenWriteBuf", len(writeBuf))
	return len(buf), nil
}

func PatchBlock(buf, patch []byte, index int64) []byte {
	// Get the length of the source buffer
	srcLen := len(buf)
	// Get the length of the patch buffer
	patchLen := int64(len(patch))

	// Create a new buffer to hold the patched data
	dst := make([]byte, srcLen)

	// Copy the first part of the source buffer to the destination buffer
	copy(dst, buf[:index])

	// Copy the patch buffer to the destination buffer at the given index
	copy(dst[index:], patch)

	// Copy the rest of the source buffer to the destination buffer
	copy(dst[index+patchLen:], buf[index+patchLen:])

	return dst
}

func NormaliseOffset(offset int64, length int64, blockSize int64) ([]int64, error) {
	startOffset := offset

	endOffset := offset + length

	blockAlignedStartOffset := floorOffset(startOffset, blockSize)
	blockAlignedEndOffset := ceilOffset(endOffset, blockSize)

	return getBlockAlignedOffsets(blockAlignedStartOffset, blockAlignedEndOffset, blockSize)
}

// floorOffset ...
func floorOffset(offset, blockSize int64) int64 {
	mod := offset % blockSize
	if mod == 0 {
		return offset
	}
	return offset - mod
}

// ceilOffset ...
func ceilOffset(offset, blockSize int64) int64 {
	mod := offset % blockSize
	if mod == 0 {
		return offset
	}
	return offset + (blockSize - mod)
}

func getBlockAlignedOffsets(startOffset, endOffset, blockSize int64) ([]int64, error) {
	var blocks = make([]int64, 0)
	totalLength := endOffset - startOffset
	numBlocks := totalLength / blockSize
	reminder := totalLength % blockSize
	if reminder != 0 {
		err := fmt.Errorf("region wrong length offset at %d, of len %d", startOffset, totalLength)
		return nil, err
	}

	for i := int64(0); i < numBlocks; i++ {
		blocks = append(blocks, startOffset+i*blockSize)
	}
	return blocks, nil
}
