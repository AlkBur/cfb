package cfb

import (
	"os"
	"errors"
	"io"
	"encoding/binary"
	"bytes"
	"strings"
	"encoding/hex"
	"unicode/utf16"
	"io/ioutil"
)

const (
	MinUint uint = 0                 // binary: all zeroes
	MaxUint      = ^MinUint          // binary: all ones
	MaxInt       = int(MaxUint >> 1) // binary: all ones except high bit
	MinInt       = ^MaxInt           // binary: all zeroes except high bit
)

var ErrTooLarge = errors.New("bytes buffer too large")

type File struct {
	path string
	buf []byte
	off int
}

func NewFile(filename string) (f *File, err error) {
	f = new(File)
	if filename != "" {
		err = f.Open(filename)
	}
	return
}

func (f *File)Size() int {
	return len(f.buf)
}

func (f *File)Close() {
	f.path = ""
	f.buf = f.buf[:0]
	f.off = 0
}

func (f *File)Open(filename string) (err error) {
	var file *os.File
	var info os.FileInfo

	file, err = os.Open(filename)
	if err != nil {
		return
	}
	defer file.Close()

	f.path = filename
	f.off = 0

	if info, err = os.Stat(filename); err != nil {
		f = nil
		return
	}

	if info.Size() > int64(MaxInt) {
		f = nil
		err = ErrTooLarge
		return
	}

	if f.buf == nil {
		if f.buf, err = makeSlice(int(info.Size())); err != nil {
			return
		}
	}

	if _, err = io.ReadFull(file, f.buf); err != nil {
		return
	}
	return
}

func (f *File)ReadBytes(size int) ([]byte, error) {
	if size <= 0 {
		return nil, errors.New("The size specified is less than 0")
	}
	if len(f.buf) < f.off+size {
		return nil, io.EOF
	}
	data := f.buf[f.off : f.off+size]
	f.off += size
	return data, nil
}

func (f *File) Read(b []byte) (n int, err error) {
	if f.off >= len(f.buf) {
		return 0, io.EOF
	}
	n = copy(b, f.buf[f.off:])
	f.off += n
	return
}

func (f *File) Seek(offset int, whence int) (int, error) {
	switch whence {
	case io.SeekStart:
		f.off = offset
	case io.SeekCurrent:
		f.off += offset
	case io.SeekEnd:
		f.off = len(f.buf) + offset
	}
	return f.off, nil
}

func (f *File) ReadAt(b []byte, offset int) (n int, err error) {
	if offset < 0 {
		return 0, errors.New("buffer.bytesReader.ReadAt: negative offset")
	}
	if offset >= len(f.buf) {
		return 0, io.EOF
	}
	n = copy(b, f.buf[offset:])
	if n < len(b) {
		err = io.EOF
	}
	return
}

func (f *File) Write(p []byte) (n int, err error) {
	n = copy(f.buf[f.off:], p)
	f.off += n
	return
}

func (f *File) Save() error {
	return ioutil.WriteFile(f.path, f.buf, 0644)
}

/////////////////////////////////////////////////

func (f *File) deleteByte(offset int) {
	copy(f.buf[offset:], f.buf[offset+1:])
	f.buf = f.buf[:len(f.buf)-1]
}

func (f *File) appendByte(b byte) {
	f.buf = append(f.buf, b)
}

func (f *File) replaceByte(offset int64, b byte) {
	f.buf[offset] = b
}

func makeSlice(n int) (b []byte, err error) {
	defer func() {
		if recover() != nil {
			err = ErrTooLarge
		}
	}()
	b = make([]byte, n)
	return
}

// convert a sector to an array of 32 bits unsigned integers,
func sect2array(sect []byte) (val []uint32, err error) {
	val = make([]uint32, len(sect)/4)
	buf := bytes.NewBuffer(sect)
	err = binary.Read(buf, binary.LittleEndian, &val)
	return
}

func decode_utf16_str(b []uint16) string {
	return string(utf16.Decode(b))
}


type Guid struct {
	DataA uint32
	DataB uint16
	DataC uint16
	DataD [8]byte
}

func (g Guid) String() string {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[:4], g.DataA)
	binary.BigEndian.PutUint16(buf[4:6], g.DataB)
	binary.BigEndian.PutUint16(buf[6:], g.DataC)
	return strings.ToUpper("{" +
		hex.EncodeToString(buf[:4]) +
		"-" +
		hex.EncodeToString(buf[4:6]) +
		"-" +
		hex.EncodeToString(buf[6:]) +
		"-" +
		hex.EncodeToString(g.DataD[:2]) +
		"-" +
		hex.EncodeToString(g.DataD[2:]) +
		"}")
}

type FileTime struct {
	Low  uint32 // Windows FILETIME structure
	High uint32 // Windows FILETIME structure
}

