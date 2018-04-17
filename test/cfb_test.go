package test

import (
	"testing"
	"github.com/AlkBur/cfb"
	"github.com/stretchr/testify/assert"
	"os"
	"io"
	"bytes"
)

const (
	non_ole_file = "files/flower.jpg"
	ole_file = "files/test-ole-file.doc"
	file_1c = "files/1Cv7.MD"
)

func TestIsOlefile(t *testing.T){
	asserts := assert.New(t)

	is_ole, err := cfb.IsOleFile(non_ole_file)
	asserts.NoError(err, "ole file: %v", non_ole_file)
	asserts.False(is_ole, non_ole_file)

	is_ole, err = cfb.IsOleFile(ole_file)
	asserts.NoError(err, "ole file: %v", ole_file)
	asserts.True(is_ole, ole_file)
}

func TestExistsWordDocument(t *testing.T){
	asserts := assert.New(t)

	ole, err := cfb.NewOleFile(ole_file)
	asserts.NoError(err, "ole file: %v", ole_file)
	exists := ole.Exists("worddocument")
	asserts.True(exists, ole_file)
	ole.Close()
}

func TestExists1C(t *testing.T){
	asserts := assert.New(t)

	ole, err := cfb.NewOleFile(file_1c)
	asserts.NoError(err, "ole file: %v", file_1c)
	exists := ole.Exists("worddocument")
	asserts.False(exists, file_1c)
	ole.Close()
}

func TestExistsNoVBAMacros(t *testing.T) {
	asserts := assert.New(t)

	ole, err := cfb.NewOleFile(ole_file)
	asserts.NoError(err, "ole file: %v", ole_file)
	exists := ole.Exists("macros/vba")
	asserts.False(exists)
	ole.Close()
}

func Test_Get_Type(t *testing.T){
	asserts := assert.New(t)

	ole, err := cfb.NewOleFile(ole_file)
	asserts.NoError(err, "ole file: %v", ole_file)
	doc_type := ole.GetType("worddocument")
	asserts.Equal(doc_type, cfb.STGTY_STREAM)
	ole.Close()
}

func Test_Get_Size(t *testing.T) {
	asserts := assert.New(t)

	ole, err := cfb.NewOleFile(ole_file)
	asserts.NoError(err, "ole file: %v", ole_file)
	size := ole.GetSize("worddocument")
	asserts.Condition(func() (success bool) {
		return size > 0
	})
	ole.Close()
}

func Test_Get_Rootentry_Name(t *testing.T) {
	asserts := assert.New(t)

	ole, err := cfb.NewOleFile(ole_file)
	asserts.NoError(err, "ole file: %v", ole_file)
	root := ole.GetRootentryName()
	asserts.Equal(root, "Root Entry")
	ole.Close()
}

func Test_Minifat_Writing(t *testing.T) {
	asserts := assert.New(t)

	ole_file_copy := "files/test-ole-file-copy.doc"
	minifat_stream_name := "compobj"

	if _, err := os.Stat(ole_file_copy); err == nil {
		os.Remove(ole_file_copy)
	}
	err := CopyFile(ole_file, ole_file_copy)
	asserts.NoError(err, "ole file: %v", ole_file)
	defer os.Remove(ole_file_copy)

	ole, err := cfb.NewOleFile(ole_file_copy)
	asserts.NoError(err, "ole file: %v", ole_file_copy)
	stream, err := ole.OpenStream(minifat_stream_name)
	asserts.NoError(err, "ole file: %v", ole_file_copy)
	asserts.Condition(func() (success bool) {
		return stream.Size() < int(ole.GetHeader().MiniStreamCutoffSize)
	})

	str_read := make([]byte, stream.Size())
	n, err := stream.Read(str_read)
	asserts.NoError(err, "ole file: %v", ole_file_copy)
	asserts.Condition(func() (success bool) {
		return n == stream.Size()
	})

	str_empty := make([]byte, stream.Size())
	assert.NotEqual(t, str_read, str_empty, "bytes")
	stream.Close()


	err = ole.WriteStream(minifat_stream_name, str_empty)
	asserts.NoError(err, "ole file: %v", ole_file_copy)
	err = ole.Save()
	asserts.NoError(err, "ole file: %v", ole_file_copy)
	ole.Close()

	ole, err = cfb.NewOleFile(ole_file_copy)
	asserts.NoError(err, "ole file: %v", ole_file_copy)
	stream, err = ole.OpenStream(minifat_stream_name)
	asserts.NoError(err, "ole file: %v", ole_file_copy)
	asserts.Condition(func() (success bool) {
		return stream.Size() < int(ole.GetHeader().MiniStreamCutoffSize)
	})
	str_read_replaced := make([]byte, stream.Size())
	n, err = stream.Read(str_read_replaced)
	asserts.NoError(err, "ole file: %v", ole_file_copy)
	asserts.Condition(func() (success bool) {
		return n == stream.Size()
	})
	assert.False(t, bytes.Equal(str_read_replaced, str_read))
	assert.True(t, bytes.Equal(str_read_replaced, str_empty))
	stream.Close()
	ole.Close()
}

func CopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}