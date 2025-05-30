// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package arvados

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	check "gopkg.in/check.v1"
)

var _ = check.Suite(&CollectionFSSuite{})

type keepClientStub struct {
	blocks      map[string][]byte
	refreshable map[string]bool
	cached      map[string]bool
	reads       []string                   // locators from ReadAt() calls
	onWrite     func(bufcopy []byte) error // called from WriteBlock, before acquiring lock
	authToken   string                     // client's auth token (used for signing locators)
	sigkey      string                     // blob signing key
	sigttl      time.Duration              // blob signing ttl
	sync.RWMutex
}

var errStub404 = errors.New("404 block not found")

func (kcs *keepClientStub) ReadAt(locator string, p []byte, off int) (int, error) {
	kcs.Lock()
	kcs.reads = append(kcs.reads, locator)
	kcs.Unlock()
	kcs.RLock()
	defer kcs.RUnlock()
	if err := VerifySignature(locator, kcs.authToken, kcs.sigttl, []byte(kcs.sigkey)); err != nil {
		return 0, err
	}
	buf := kcs.blocks[locator[:32]]
	if buf == nil {
		return 0, errStub404
	}
	return copy(p, buf[off:]), nil
}

func (kcs *keepClientStub) BlockRead(_ context.Context, opts BlockReadOptions) (int, error) {
	kcs.Lock()
	kcs.reads = append(kcs.reads, opts.Locator)
	kcs.Unlock()
	kcs.RLock()
	defer kcs.RUnlock()
	if opts.CheckCacheOnly {
		if kcs.cached[opts.Locator[:32]] {
			return 0, nil
		} else {
			return 0, ErrNotCached
		}
	}
	if err := VerifySignature(opts.Locator, kcs.authToken, kcs.sigttl, []byte(kcs.sigkey)); err != nil {
		return 0, err
	}
	buf := kcs.blocks[opts.Locator[:32]]
	if buf == nil {
		return 0, errStub404
	}
	n, err := io.Copy(opts.WriteTo, bytes.NewReader(buf))
	return int(n), err
}

func (kcs *keepClientStub) BlockWrite(_ context.Context, opts BlockWriteOptions) (BlockWriteResponse, error) {
	var buf []byte
	if opts.Data == nil {
		buf = make([]byte, opts.DataSize)
		_, err := io.ReadFull(opts.Reader, buf)
		if err != nil {
			return BlockWriteResponse{}, err
		}
	} else {
		buf = append([]byte(nil), opts.Data...)
	}
	locator := SignLocator(fmt.Sprintf("%x+%d", md5.Sum(buf), len(buf)), kcs.authToken, time.Now().Add(kcs.sigttl), kcs.sigttl, []byte(kcs.sigkey))
	if kcs.onWrite != nil {
		err := kcs.onWrite(buf)
		if err != nil {
			return BlockWriteResponse{}, err
		}
	}
	for _, sc := range opts.StorageClasses {
		if sc != "default" {
			return BlockWriteResponse{}, fmt.Errorf("stub does not write storage class %q", sc)
		}
	}
	kcs.Lock()
	defer kcs.Unlock()
	kcs.blocks[locator[:32]] = buf
	return BlockWriteResponse{Locator: locator, Replicas: 1}, nil
}

var reRemoteSignature = regexp.MustCompile(`\+[AR][^+]*`)

func (kcs *keepClientStub) LocalLocator(locator string) (string, error) {
	if strings.Contains(locator, "+A") {
		return locator, nil
	}
	kcs.Lock()
	defer kcs.Unlock()
	if strings.Contains(locator, "+R") {
		if len(locator) < 32 {
			return "", fmt.Errorf("bad locator: %q", locator)
		}
		if _, ok := kcs.blocks[locator[:32]]; !ok && !kcs.refreshable[locator[:32]] {
			return "", fmt.Errorf("kcs.refreshable[%q]==false", locator)
		}
	}
	locator = reRemoteSignature.ReplaceAllLiteralString(locator, "")
	locator = SignLocator(locator, kcs.authToken, time.Now().Add(kcs.sigttl), kcs.sigttl, []byte(kcs.sigkey))
	return locator, nil
}

type CollectionFSSuite struct {
	client *Client
	coll   Collection
	fs     CollectionFileSystem
	kc     *keepClientStub
}

func (s *CollectionFSSuite) SetUpTest(c *check.C) {
	s.client = NewClientFromEnv()
	s.client.AuthToken = fixtureActiveToken
	err := s.client.RequestAndDecode(&s.coll, "GET", "arvados/v1/collections/"+fixtureFooAndBarFilesInDirUUID, nil, nil)
	c.Assert(err, check.IsNil)
	s.kc = &keepClientStub{
		blocks: map[string][]byte{
			"3858f62230ac3c915f300c664312c63f": []byte("foobar"),
		},
		sigkey:    fixtureBlobSigningKey,
		sigttl:    fixtureBlobSigningTTL,
		authToken: fixtureActiveToken,
	}
	s.fs, err = s.coll.FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)
}

func (s *CollectionFSSuite) TestSyncNonCanonicalManifest(c *check.C) {
	var coll Collection
	err := s.client.RequestAndDecode(&coll, "GET", "arvados/v1/collections/"+fixtureFooAndBarFilesInDirUUID, nil, nil)
	c.Assert(err, check.IsNil)
	mtxt := strings.Replace(coll.ManifestText, "3:3:bar 0:3:foo", "0:3:foo 3:3:bar", -1)
	c.Assert(mtxt, check.Not(check.Equals), coll.ManifestText)
	err = s.client.RequestAndDecode(&coll, "POST", "arvados/v1/collections", nil, map[string]interface{}{
		"collection": map[string]interface{}{
			"manifest_text": mtxt}})
	c.Assert(err, check.IsNil)
	// In order for the rest of the test to work as intended, the API server
	// needs to retain the file ordering we set manually. We check that here.
	// We can't check `mtxt == coll.ManifestText` because the API server
	// might've returned new block signatures if the GET and POST happened in
	// different seconds.
	expectPattern := `\./dir1 \S+ 0:3:foo 3:3:bar\n`
	c.Assert(coll.ManifestText, check.Matches, expectPattern)

	fs, err := coll.FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)
	err = fs.Sync()
	c.Check(err, check.IsNil)

	// fs had no local changes, so Sync should not have saved
	// anything back to the API/database. (If it did, we would see
	// the manifest rewritten in canonical order.)
	var saved Collection
	err = s.client.RequestAndDecode(&saved, "GET", "arvados/v1/collections/"+coll.UUID, nil, nil)
	c.Assert(err, check.IsNil)
	c.Check(saved.ManifestText, check.Matches, expectPattern)
}

func (s *CollectionFSSuite) TestHttpFileSystemInterface(c *check.C) {
	_, ok := s.fs.(http.FileSystem)
	c.Check(ok, check.Equals, true)
}

func (s *CollectionFSSuite) TestUnattainableStorageClasses(c *check.C) {
	fs, err := (&Collection{
		StorageClassesDesired: []string{"unobtainium"},
	}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)

	f, err := fs.OpenFile("/foo", os.O_CREATE|os.O_WRONLY, 0777)
	c.Assert(err, check.IsNil)
	_, err = f.Write([]byte("food"))
	c.Assert(err, check.IsNil)
	err = f.Close()
	c.Assert(err, check.IsNil)
	_, err = fs.MarshalManifest(".")
	c.Assert(err, check.ErrorMatches, `.*stub does not write storage class \"unobtainium\"`)
}

func (s *CollectionFSSuite) TestColonInFilename(c *check.C) {
	fs, err := (&Collection{
		ManifestText: "./foo:foo 3858f62230ac3c915f300c664312c63f+3 0:3:bar:bar\n",
	}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)

	f, err := fs.Open("/foo:foo")
	c.Assert(err, check.IsNil)

	fis, err := f.Readdir(0)
	c.Check(err, check.IsNil)
	c.Check(len(fis), check.Equals, 1)
	c.Check(fis[0].Name(), check.Equals, "bar:bar")
}

func (s *CollectionFSSuite) TestReaddirFull(c *check.C) {
	f, err := s.fs.Open("/dir1")
	c.Assert(err, check.IsNil)

	st, err := f.Stat()
	c.Assert(err, check.IsNil)
	c.Check(st.Size(), check.Equals, int64(2))
	c.Check(st.IsDir(), check.Equals, true)

	fis, err := f.Readdir(0)
	c.Check(err, check.IsNil)
	c.Check(len(fis), check.Equals, 2)
	if len(fis) > 0 {
		c.Check(fis[0].Size(), check.Equals, int64(3))
	}
}

func (s *CollectionFSSuite) TestReaddirLimited(c *check.C) {
	f, err := s.fs.Open("./dir1")
	c.Assert(err, check.IsNil)

	fis, err := f.Readdir(1)
	c.Check(err, check.IsNil)
	c.Check(len(fis), check.Equals, 1)
	if len(fis) > 0 {
		c.Check(fis[0].Size(), check.Equals, int64(3))
	}

	fis, err = f.Readdir(1)
	c.Check(err, check.IsNil)
	c.Check(len(fis), check.Equals, 1)
	if len(fis) > 0 {
		c.Check(fis[0].Size(), check.Equals, int64(3))
	}

	fis, err = f.Readdir(1)
	c.Check(len(fis), check.Equals, 0)
	c.Check(err, check.NotNil)
	c.Check(err, check.Equals, io.EOF)

	f, err = s.fs.Open("dir1")
	c.Assert(err, check.IsNil)
	fis, err = f.Readdir(1)
	c.Check(len(fis), check.Equals, 1)
	c.Assert(err, check.IsNil)
	fis, err = f.Readdir(2)
	c.Check(len(fis), check.Equals, 1)
	c.Assert(err, check.IsNil)
	fis, err = f.Readdir(2)
	c.Check(len(fis), check.Equals, 0)
	c.Assert(err, check.Equals, io.EOF)
}

func (s *CollectionFSSuite) TestPathMunge(c *check.C) {
	for _, path := range []string{".", "/", "./", "///", "/../", "/./.."} {
		f, err := s.fs.Open(path)
		c.Assert(err, check.IsNil)

		st, err := f.Stat()
		c.Assert(err, check.IsNil)
		c.Check(st.Size(), check.Equals, int64(1))
		c.Check(st.IsDir(), check.Equals, true)
	}
	for _, path := range []string{"/dir1", "dir1", "./dir1", "///dir1//.//", "../dir1/../dir1/"} {
		c.Logf("%q", path)
		f, err := s.fs.Open(path)
		c.Assert(err, check.IsNil)

		st, err := f.Stat()
		c.Assert(err, check.IsNil)
		c.Check(st.Size(), check.Equals, int64(2))
		c.Check(st.IsDir(), check.Equals, true)
	}
}

func (s *CollectionFSSuite) TestNotExist(c *check.C) {
	for _, path := range []string{"/no", "no", "./no", "n/o", "/n/o"} {
		f, err := s.fs.Open(path)
		c.Assert(f, check.IsNil)
		c.Assert(err, check.NotNil)
		c.Assert(os.IsNotExist(err), check.Equals, true)
	}
}

func (s *CollectionFSSuite) TestReadOnlyFile(c *check.C) {
	f, err := s.fs.OpenFile("/dir1/foo", os.O_RDONLY, 0)
	c.Assert(err, check.IsNil)
	st, err := f.Stat()
	c.Assert(err, check.IsNil)
	c.Check(st.Size(), check.Equals, int64(3))
	n, err := f.Write([]byte("bar"))
	c.Check(n, check.Equals, 0)
	c.Check(err, check.Equals, ErrReadOnlyFile)
}

func (s *CollectionFSSuite) TestCreateFile(c *check.C) {
	f, err := s.fs.OpenFile("/new-file 1", os.O_RDWR|os.O_CREATE, 0)
	c.Assert(err, check.IsNil)
	st, err := f.Stat()
	c.Assert(err, check.IsNil)
	c.Check(st.Size(), check.Equals, int64(0))

	n, err := f.Write([]byte("bar"))
	c.Check(n, check.Equals, 3)
	c.Check(err, check.IsNil)

	c.Check(f.Close(), check.IsNil)

	f, err = s.fs.OpenFile("/new-file 1", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0)
	c.Check(f, check.IsNil)
	c.Assert(err, check.NotNil)

	f, err = s.fs.OpenFile("/new-file 1", os.O_RDWR, 0)
	c.Assert(err, check.IsNil)
	st, err = f.Stat()
	c.Assert(err, check.IsNil)
	c.Check(st.Size(), check.Equals, int64(3))

	c.Check(f.Close(), check.IsNil)

	m, err := s.fs.MarshalManifest(".")
	c.Assert(err, check.IsNil)
	c.Check(m, check.Matches, `. 37b51d194a7513e45b56f6524f2d51f2\+3\+\S+ 0:3:new-file\\0401\n./dir1 .* 3:3:bar 0:3:foo\n`)
}

func (s *CollectionFSSuite) TestReadWriteFile(c *check.C) {
	maxBlockSize = 8
	defer func() { maxBlockSize = 1 << 26 }()

	f, err := s.fs.OpenFile("/dir1/foo", os.O_RDWR, 0)
	c.Assert(err, check.IsNil)
	defer f.Close()
	st, err := f.Stat()
	c.Assert(err, check.IsNil)
	c.Check(st.Size(), check.Equals, int64(3))

	f2, err := s.fs.OpenFile("/dir1/foo", os.O_RDWR, 0)
	c.Assert(err, check.IsNil)
	defer f2.Close()

	buf := make([]byte, 64)
	n, err := f.Read(buf)
	c.Check(n, check.Equals, 3)
	c.Check(err, check.Equals, io.EOF)
	c.Check(string(buf[:3]), check.DeepEquals, "foo")

	pos, err := f.Seek(-2, io.SeekCurrent)
	c.Check(pos, check.Equals, int64(1))
	c.Check(err, check.IsNil)

	// Split a storedExtent in two, and insert a memExtent
	n, err = f.Write([]byte("*"))
	c.Check(n, check.Equals, 1)
	c.Check(err, check.IsNil)

	pos, err = f.Seek(0, io.SeekCurrent)
	c.Check(pos, check.Equals, int64(2))
	c.Check(err, check.IsNil)

	pos, err = f.Seek(0, io.SeekStart)
	c.Check(pos, check.Equals, int64(0))
	c.Check(err, check.IsNil)

	rbuf, err := ioutil.ReadAll(f)
	c.Check(len(rbuf), check.Equals, 3)
	c.Check(err, check.IsNil)
	c.Check(string(rbuf), check.Equals, "f*o")

	// Write multiple blocks in one call
	f.Seek(1, io.SeekStart)
	n, err = f.Write([]byte("0123456789abcdefg"))
	c.Check(n, check.Equals, 17)
	c.Check(err, check.IsNil)
	pos, err = f.Seek(0, io.SeekCurrent)
	c.Check(pos, check.Equals, int64(18))
	c.Check(err, check.IsNil)
	pos, err = f.Seek(-18, io.SeekCurrent)
	c.Check(pos, check.Equals, int64(0))
	c.Check(err, check.IsNil)
	n, err = io.ReadFull(f, buf)
	c.Check(n, check.Equals, 18)
	c.Check(err, check.Equals, io.ErrUnexpectedEOF)
	c.Check(string(buf[:n]), check.Equals, "f0123456789abcdefg")

	buf2, err := ioutil.ReadAll(f2)
	c.Check(err, check.IsNil)
	c.Check(string(buf2), check.Equals, "f0123456789abcdefg")

	// truncate to current size
	err = f.Truncate(18)
	c.Check(err, check.IsNil)
	f2.Seek(0, io.SeekStart)
	buf2, err = ioutil.ReadAll(f2)
	c.Check(err, check.IsNil)
	c.Check(string(buf2), check.Equals, "f0123456789abcdefg")

	// shrink to zero some data
	f.Truncate(15)
	f2.Seek(0, io.SeekStart)
	buf2, err = ioutil.ReadAll(f2)
	c.Check(err, check.IsNil)
	c.Check(string(buf2), check.Equals, "f0123456789abcd")

	// grow to partial block/extent
	f.Truncate(20)
	f2.Seek(0, io.SeekStart)
	buf2, err = ioutil.ReadAll(f2)
	c.Check(err, check.IsNil)
	c.Check(string(buf2), check.Equals, "f0123456789abcd\x00\x00\x00\x00\x00")

	f.Truncate(0)
	f2.Seek(0, io.SeekStart)
	f2.Write([]byte("12345678abcdefghijkl"))

	// grow to block/extent boundary
	f.Truncate(64)
	f2.Seek(0, io.SeekStart)
	buf2, err = ioutil.ReadAll(f2)
	c.Check(err, check.IsNil)
	c.Check(len(buf2), check.Equals, 64)
	c.Check(len(f.(*filehandle).inode.(*filenode).segments), check.Equals, 8)

	// shrink to block/extent boundary
	err = f.Truncate(32)
	c.Check(err, check.IsNil)
	f2.Seek(0, io.SeekStart)
	buf2, err = ioutil.ReadAll(f2)
	c.Check(err, check.IsNil)
	c.Check(len(buf2), check.Equals, 32)
	c.Check(len(f.(*filehandle).inode.(*filenode).segments), check.Equals, 4)

	// shrink to partial block/extent
	err = f.Truncate(15)
	c.Check(err, check.IsNil)
	f2.Seek(0, io.SeekStart)
	buf2, err = ioutil.ReadAll(f2)
	c.Check(err, check.IsNil)
	c.Check(string(buf2), check.Equals, "12345678abcdefg")
	c.Check(len(f.(*filehandle).inode.(*filenode).segments), check.Equals, 2)

	// Force flush to ensure the block "12345678" gets stored, so
	// we know what to expect in the final manifest below.
	_, err = s.fs.MarshalManifest(".")
	c.Check(err, check.IsNil)

	// Truncate to size=3 while f2's ptr is at 15
	err = f.Truncate(3)
	c.Check(err, check.IsNil)
	buf2, err = ioutil.ReadAll(f2)
	c.Check(err, check.IsNil)
	c.Check(string(buf2), check.Equals, "")
	f2.Seek(0, io.SeekStart)
	buf2, err = ioutil.ReadAll(f2)
	c.Check(err, check.IsNil)
	c.Check(string(buf2), check.Equals, "123")
	c.Check(len(f.(*filehandle).inode.(*filenode).segments), check.Equals, 1)

	m, err := s.fs.MarshalManifest(".")
	c.Check(err, check.IsNil)
	m = regexp.MustCompile(`\+A[^\+ ]+`).ReplaceAllLiteralString(m, "")
	c.Check(m, check.Equals, "./dir1 3858f62230ac3c915f300c664312c63f+6 25d55ad283aa400af464c76d713c07ad+8 3:3:bar 6:3:foo\n")
	c.Check(s.fs.Size(), check.Equals, int64(6))
}

func (s *CollectionFSSuite) TestSeekSparse(c *check.C) {
	fs, err := (&Collection{}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)
	f, err := fs.OpenFile("test", os.O_CREATE|os.O_RDWR, 0755)
	c.Assert(err, check.IsNil)
	defer f.Close()

	checkSize := func(size int64) {
		fi, err := f.Stat()
		c.Assert(err, check.IsNil)
		c.Check(fi.Size(), check.Equals, size)

		f, err := fs.OpenFile("test", os.O_CREATE|os.O_RDWR, 0755)
		c.Assert(err, check.IsNil)
		defer f.Close()
		fi, err = f.Stat()
		c.Check(err, check.IsNil)
		c.Check(fi.Size(), check.Equals, size)
		pos, err := f.Seek(0, io.SeekEnd)
		c.Check(err, check.IsNil)
		c.Check(pos, check.Equals, size)
	}

	f.Seek(2, io.SeekEnd)
	checkSize(0)
	f.Write([]byte{1})
	checkSize(3)

	f.Seek(2, io.SeekCurrent)
	checkSize(3)
	f.Write([]byte{})
	checkSize(5)

	f.Seek(8, io.SeekStart)
	checkSize(5)
	n, err := f.Read(make([]byte, 1))
	c.Check(n, check.Equals, 0)
	c.Check(err, check.Equals, io.EOF)
	checkSize(5)
	f.Write([]byte{1, 2, 3})
	checkSize(11)
}

func (s *CollectionFSSuite) TestMarshalCopiesRemoteBlocks(c *check.C) {
	foo := "foo"
	bar := "bar"
	hash := map[string]string{
		foo: fmt.Sprintf("%x", md5.Sum([]byte(foo))),
		bar: fmt.Sprintf("%x", md5.Sum([]byte(bar))),
	}

	fs, err := (&Collection{
		ManifestText: ". " + hash[foo] + "+3+Rzaaaa-foo@bab " + hash[bar] + "+3+A12345@ffffff 0:2:fo.txt 2:4:obar.txt\n",
	}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)
	manifest, err := fs.MarshalManifest(".")
	c.Check(manifest, check.Equals, "")
	c.Check(err, check.NotNil)

	s.kc.refreshable = map[string]bool{hash[bar]: true}

	for _, sigIn := range []string{"Rzaaaa-foo@bab", "A12345@abcde"} {
		fs, err = (&Collection{
			ManifestText: ". " + hash[foo] + "+3+A12345@fffff " + hash[bar] + "+3+" + sigIn + " 0:2:fo.txt 2:4:obar.txt\n",
		}).FileSystem(s.client, s.kc)
		c.Assert(err, check.IsNil)
		manifest, err := fs.MarshalManifest(".")
		c.Check(err, check.IsNil)
		// Both blocks should now have +A signatures.
		c.Check(manifest, check.Matches, `.*\+A.* .*\+A.*\n`)
		c.Check(manifest, check.Not(check.Matches), `.*\+R.*\n`)
	}
}

func (s *CollectionFSSuite) TestMarshalSmallBlocks(c *check.C) {
	maxBlockSize = 8
	defer func() { maxBlockSize = 1 << 26 }()

	var err error
	s.fs, err = (&Collection{}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)
	for _, name := range []string{"foo", "bar", "baz"} {
		f, err := s.fs.OpenFile(name, os.O_WRONLY|os.O_CREATE, 0)
		c.Assert(err, check.IsNil)
		f.Write([]byte(name))
		f.Close()
	}

	m, err := s.fs.MarshalManifest(".")
	c.Check(err, check.IsNil)
	m = regexp.MustCompile(`\+A[^\+ ]+`).ReplaceAllLiteralString(m, "")
	c.Check(m, check.Equals, ". c3c23db5285662ef7172373df0003206+6 acbd18db4cc2f85cedef654fccc4a4d8+3 0:3:bar 3:3:baz 6:3:foo\n")
}

func (s *CollectionFSSuite) TestMkdir(c *check.C) {
	err := s.fs.Mkdir("foo/bar", 0755)
	c.Check(err, check.Equals, os.ErrNotExist)

	f, err := s.fs.OpenFile("foo/bar", os.O_CREATE, 0)
	c.Check(err, check.Equals, os.ErrNotExist)

	err = s.fs.Mkdir("foo", 0755)
	c.Check(err, check.IsNil)

	f, err = s.fs.OpenFile("foo/bar", os.O_CREATE|os.O_WRONLY, 0)
	c.Check(err, check.IsNil)
	if err == nil {
		defer f.Close()
		f.Write([]byte("foo"))
	}

	// mkdir fails if a file already exists with that name
	err = s.fs.Mkdir("foo/bar", 0755)
	c.Check(err, check.NotNil)

	err = s.fs.Remove("foo/bar")
	c.Check(err, check.IsNil)

	// mkdir succeeds after the file is deleted
	err = s.fs.Mkdir("foo/bar", 0755)
	c.Check(err, check.IsNil)

	// creating a file in a nonexistent subdir should still fail
	f, err = s.fs.OpenFile("foo/bar/baz/foo.txt", os.O_CREATE|os.O_WRONLY, 0)
	c.Check(err, check.Equals, os.ErrNotExist)

	f, err = s.fs.OpenFile("foo/bar/foo.txt", os.O_CREATE|os.O_WRONLY, 0)
	c.Check(err, check.IsNil)
	if err == nil {
		defer f.Close()
		f.Write([]byte("foo"))
	}

	// creating foo/bar as a regular file should fail
	f, err = s.fs.OpenFile("foo/bar", os.O_CREATE|os.O_EXCL, 0)
	c.Check(err, check.NotNil)

	// creating foo/bar as a directory should fail
	f, err = s.fs.OpenFile("foo/bar", os.O_CREATE|os.O_EXCL, os.ModeDir)
	c.Check(err, check.NotNil)
	err = s.fs.Mkdir("foo/bar", 0755)
	c.Check(err, check.NotNil)

	m, err := s.fs.MarshalManifest(".")
	c.Check(err, check.IsNil)
	m = regexp.MustCompile(`\+A[^\+ ]+`).ReplaceAllLiteralString(m, "")
	c.Check(m, check.Equals, "./dir1 3858f62230ac3c915f300c664312c63f+6 3:3:bar 0:3:foo\n./foo/bar acbd18db4cc2f85cedef654fccc4a4d8+3 0:3:foo.txt\n")
}

func (s *CollectionFSSuite) TestConcurrentWriters(c *check.C) {
	if testing.Short() {
		c.Skip("slow")
	}

	maxBlockSize = 8
	defer func() { maxBlockSize = 1 << 26 }()

	var wg sync.WaitGroup
	for n := 0; n < 128; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f, err := s.fs.OpenFile("/dir1/foo", os.O_RDWR, 0)
			c.Assert(err, check.IsNil)
			defer f.Close()
			for i := 0; i < 1024; i++ {
				r := rand.Uint32()
				switch {
				case r%11 == 0:
					_, err := s.fs.MarshalManifest(".")
					c.Check(err, check.IsNil)
				case r&3 == 0:
					f.Truncate(int64(rand.Intn(64)))
				case r&3 == 1:
					f.Seek(int64(rand.Intn(64)), io.SeekStart)
				case r&3 == 2:
					_, err := f.Write([]byte("beep boop"))
					c.Check(err, check.IsNil)
				case r&3 == 3:
					_, err := ioutil.ReadAll(f)
					c.Check(err, check.IsNil)
				}
			}
		}()
	}
	wg.Wait()

	f, err := s.fs.OpenFile("/dir1/foo", os.O_RDWR, 0)
	c.Assert(err, check.IsNil)
	defer f.Close()
	buf, err := ioutil.ReadAll(f)
	c.Check(err, check.IsNil)
	c.Logf("after lots of random r/w/seek/trunc, buf is %q", buf)
}

func (s *CollectionFSSuite) TestRandomWrites(c *check.C) {
	maxBlockSize = 40
	defer func() { maxBlockSize = 1 << 26 }()

	var err error
	s.fs, err = (&Collection{}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)

	const nfiles = 256
	const ngoroutines = 256

	var wg sync.WaitGroup
	for n := 0; n < ngoroutines; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			expect := make([]byte, 0, 64)
			wbytes := []byte("there's no simple explanation for anything important that any of us do")
			f, err := s.fs.OpenFile(fmt.Sprintf("random-%d", n), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0)
			c.Assert(err, check.IsNil)
			defer f.Close()
			for i := 0; i < nfiles; i++ {
				trunc := rand.Intn(65)
				woff := rand.Intn(trunc + 1)
				wbytes = wbytes[:rand.Intn(64-woff+1)]
				for buf, i := expect[:cap(expect)], len(expect); i < trunc; i++ {
					buf[i] = 0
				}
				expect = expect[:trunc]
				if trunc < woff+len(wbytes) {
					expect = expect[:woff+len(wbytes)]
				}
				copy(expect[woff:], wbytes)
				f.Truncate(int64(trunc))
				pos, err := f.Seek(int64(woff), io.SeekStart)
				c.Check(pos, check.Equals, int64(woff))
				c.Check(err, check.IsNil)
				n, err := f.Write(wbytes)
				c.Check(n, check.Equals, len(wbytes))
				c.Check(err, check.IsNil)
				pos, err = f.Seek(0, io.SeekStart)
				c.Check(pos, check.Equals, int64(0))
				c.Check(err, check.IsNil)
				buf, err := ioutil.ReadAll(f)
				c.Check(string(buf), check.Equals, string(expect))
				c.Check(err, check.IsNil)
			}
		}(n)
	}
	wg.Wait()

	for n := 0; n < ngoroutines; n++ {
		f, err := s.fs.OpenFile(fmt.Sprintf("random-%d", n), os.O_RDONLY, 0)
		c.Assert(err, check.IsNil)
		f.(*filehandle).inode.(*filenode).waitPrune()
		s.checkMemSize(c, f)
		defer f.Close()
	}

	root, err := s.fs.Open("/")
	c.Assert(err, check.IsNil)
	defer root.Close()
	fi, err := root.Readdir(-1)
	c.Check(err, check.IsNil)
	c.Check(len(fi), check.Equals, nfiles)

	_, err = s.fs.MarshalManifest(".")
	c.Check(err, check.IsNil)
	// TODO: check manifest content
}

func (s *CollectionFSSuite) TestRemove(c *check.C) {
	fs, err := (&Collection{}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)
	err = fs.Mkdir("dir0", 0755)
	c.Assert(err, check.IsNil)
	err = fs.Mkdir("dir1", 0755)
	c.Assert(err, check.IsNil)
	err = fs.Mkdir("dir1/dir2", 0755)
	c.Assert(err, check.IsNil)
	err = fs.Mkdir("dir1/dir3", 0755)
	c.Assert(err, check.IsNil)

	err = fs.Remove("dir0")
	c.Check(err, check.IsNil)
	err = fs.Remove("dir0")
	c.Check(err, check.Equals, os.ErrNotExist)

	err = fs.Remove("dir1/dir2/.")
	c.Check(err, check.Equals, ErrInvalidArgument)
	err = fs.Remove("dir1/dir2/..")
	c.Check(err, check.Equals, ErrInvalidArgument)
	err = fs.Remove("dir1")
	c.Check(err, check.Equals, ErrDirectoryNotEmpty)
	err = fs.Remove("dir1/dir2/../../../dir1")
	c.Check(err, check.Equals, ErrDirectoryNotEmpty)
	err = fs.Remove("dir1/dir3/")
	c.Check(err, check.IsNil)
	err = fs.RemoveAll("dir1")
	c.Check(err, check.IsNil)
	err = fs.RemoveAll("dir1")
	c.Check(err, check.IsNil)
}

func (s *CollectionFSSuite) TestRenameError(c *check.C) {
	fs, err := (&Collection{}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)
	err = fs.Mkdir("first", 0755)
	c.Assert(err, check.IsNil)
	err = fs.Mkdir("first/second", 0755)
	c.Assert(err, check.IsNil)
	f, err := fs.OpenFile("first/second/file", os.O_CREATE|os.O_WRONLY, 0755)
	c.Assert(err, check.IsNil)
	f.Write([]byte{1, 2, 3, 4, 5})
	f.Close()
	err = fs.Rename("first", "first/second/third")
	c.Check(err, check.Equals, ErrInvalidArgument)
	err = fs.Rename("first", "first/third")
	c.Check(err, check.Equals, ErrInvalidArgument)
	err = fs.Rename("first/second", "second")
	c.Check(err, check.IsNil)
	f, err = fs.OpenFile("second/file", 0, 0)
	c.Assert(err, check.IsNil)
	data, err := ioutil.ReadAll(f)
	c.Check(err, check.IsNil)
	c.Check(data, check.DeepEquals, []byte{1, 2, 3, 4, 5})
}

func (s *CollectionFSSuite) TestRenameDirectory(c *check.C) {
	fs, err := (&Collection{}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)
	err = fs.Mkdir("foo", 0755)
	c.Assert(err, check.IsNil)
	err = fs.Mkdir("bar", 0755)
	c.Assert(err, check.IsNil)
	err = fs.Rename("bar", "baz")
	c.Check(err, check.IsNil)
	err = fs.Rename("foo", "baz")
	c.Check(err, check.NotNil)
	err = fs.Rename("foo", "baz/")
	c.Check(err, check.IsNil)
	err = fs.Rename("baz/foo", ".")
	c.Check(err, check.Equals, ErrInvalidArgument)
	err = fs.Rename("baz/foo/", ".")
	c.Check(err, check.Equals, ErrInvalidArgument)
}

func (s *CollectionFSSuite) TestRename(c *check.C) {
	fs, err := (&Collection{}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)
	const (
		outer = 16
		inner = 16
	)
	for i := 0; i < outer; i++ {
		err = fs.Mkdir(fmt.Sprintf("dir%d", i), 0755)
		c.Assert(err, check.IsNil)
		for j := 0; j < inner; j++ {
			err = fs.Mkdir(fmt.Sprintf("dir%d/dir%d", i, j), 0755)
			c.Assert(err, check.IsNil)
			for _, fnm := range []string{
				fmt.Sprintf("dir%d/file%d", i, j),
				fmt.Sprintf("dir%d/dir%d/file%d", i, j, j),
			} {
				f, err := fs.OpenFile(fnm, os.O_CREATE|os.O_WRONLY, 0755)
				c.Assert(err, check.IsNil)
				_, err = f.Write([]byte("beep"))
				c.Assert(err, check.IsNil)
				f.Close()
			}
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < outer; i++ {
		for j := 0; j < inner; j++ {
			wg.Add(1)
			go func(i, j int) {
				defer wg.Done()
				oldname := fmt.Sprintf("dir%d/dir%d/file%d", i, j, j)
				newname := fmt.Sprintf("dir%d/newfile%d", i, inner-j-1)
				_, err := fs.Open(newname)
				c.Check(err, check.Equals, os.ErrNotExist)
				err = fs.Rename(oldname, newname)
				c.Check(err, check.IsNil)
				f, err := fs.Open(newname)
				c.Check(err, check.IsNil)
				f.Close()
			}(i, j)

			wg.Add(1)
			go func(i, j int) {
				defer wg.Done()
				// oldname does not exist
				err := fs.Rename(
					fmt.Sprintf("dir%d/dir%d/missing", i, j),
					fmt.Sprintf("dir%d/dir%d/file%d", outer-i-1, j, j))
				c.Check(err, check.ErrorMatches, `.*does not exist`)

				// newname parent dir does not exist
				err = fs.Rename(
					fmt.Sprintf("dir%d/dir%d", i, j),
					fmt.Sprintf("dir%d/missing/irrelevant", outer-i-1))
				c.Check(err, check.ErrorMatches, `.*does not exist`)

				// oldname parent dir is a file
				err = fs.Rename(
					fmt.Sprintf("dir%d/file%d/patherror", i, j),
					fmt.Sprintf("dir%d/irrelevant", i))
				c.Check(err, check.ErrorMatches, `.*not a directory`)

				// newname parent dir is a file
				err = fs.Rename(
					fmt.Sprintf("dir%d/dir%d/file%d", i, j, j),
					fmt.Sprintf("dir%d/file%d/patherror", i, inner-j-1))
				c.Check(err, check.ErrorMatches, `.*not a directory`)
			}(i, j)
		}
	}
	wg.Wait()

	f, err := fs.OpenFile("dir1/newfile3", 0, 0)
	c.Assert(err, check.IsNil)
	c.Check(f.Size(), check.Equals, int64(4))
	buf, err := ioutil.ReadAll(f)
	c.Check(buf, check.DeepEquals, []byte("beep"))
	c.Check(err, check.IsNil)
	_, err = fs.Open("dir1/dir1/file1")
	c.Check(err, check.Equals, os.ErrNotExist)
}

func (s *CollectionFSSuite) TestPersist(c *check.C) {
	maxBlockSize = 1024
	defer func() { maxBlockSize = 1 << 26 }()

	var err error
	s.fs, err = (&Collection{}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)
	err = s.fs.Mkdir("d:r", 0755)
	c.Assert(err, check.IsNil)

	expect := map[string][]byte{}

	var wg sync.WaitGroup
	for _, name := range []string{"random 1", "random:2", "random\\3", "d:r/random4"} {
		buf := make([]byte, 500)
		rand.Read(buf)
		expect[name] = buf

		f, err := s.fs.OpenFile(name, os.O_WRONLY|os.O_CREATE, 0)
		c.Assert(err, check.IsNil)
		// Note: we don't close the file until after the test
		// is done. Writes to unclosed files should persist.
		defer f.Close()

		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < len(buf); i += 5 {
				_, err := f.Write(buf[i : i+5])
				c.Assert(err, check.IsNil)
			}
		}()
	}
	wg.Wait()

	m, err := s.fs.MarshalManifest(".")
	c.Check(err, check.IsNil)
	c.Logf("%q", m)

	root, err := s.fs.Open("/")
	c.Assert(err, check.IsNil)
	defer root.Close()
	fi, err := root.Readdir(-1)
	c.Check(err, check.IsNil)
	c.Check(len(fi), check.Equals, 4)

	persisted, err := (&Collection{ManifestText: m}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)

	root, err = persisted.Open("/")
	c.Assert(err, check.IsNil)
	defer root.Close()
	fi, err = root.Readdir(-1)
	c.Check(err, check.IsNil)
	c.Check(len(fi), check.Equals, 4)

	for name, content := range expect {
		c.Logf("read %q", name)
		f, err := persisted.Open(name)
		c.Assert(err, check.IsNil)
		defer f.Close()
		buf, err := ioutil.ReadAll(f)
		c.Check(err, check.IsNil)
		c.Check(buf, check.DeepEquals, content)
	}
}

func (s *CollectionFSSuite) TestPersistEmptyFilesAndDirs(c *check.C) {
	var err error
	s.fs, err = (&Collection{}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)
	for _, name := range []string{"dir", "dir/zerodir", "empty", "not empty", "not empty/empty", "zero", "zero/zero"} {
		err = s.fs.Mkdir(name, 0755)
		c.Assert(err, check.IsNil)
	}

	expect := map[string][]byte{
		"0":                nil,
		"00":               {},
		"one":              {1},
		"dir/0":            nil,
		"dir/two":          {1, 2},
		"dir/zero":         nil,
		"dir/zerodir/zero": nil,
		"zero/zero/zero":   nil,
	}
	for name, data := range expect {
		f, err := s.fs.OpenFile(name, os.O_WRONLY|os.O_CREATE, 0)
		c.Assert(err, check.IsNil)
		if data != nil {
			_, err := f.Write(data)
			c.Assert(err, check.IsNil)
		}
		f.Close()
	}

	m, err := s.fs.MarshalManifest(".")
	c.Check(err, check.IsNil)
	c.Logf("%q", m)

	persisted, err := (&Collection{ManifestText: m}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)

	for name, data := range expect {
		_, err = persisted.Open("bogus-" + name)
		c.Check(err, check.NotNil)

		f, err := persisted.Open(name)
		c.Assert(err, check.IsNil)

		if data == nil {
			data = []byte{}
		}
		buf, err := ioutil.ReadAll(f)
		c.Check(err, check.IsNil)
		c.Check(buf, check.DeepEquals, data)
	}

	expectDir := map[string]int{
		"empty":           0,
		"not empty":       1,
		"not empty/empty": 0,
	}
	for name, expectLen := range expectDir {
		_, err := persisted.Open(name + "/bogus")
		c.Check(err, check.NotNil)

		d, err := persisted.Open(name)
		defer d.Close()
		c.Check(err, check.IsNil)
		fi, err := d.Readdir(-1)
		c.Check(err, check.IsNil)
		c.Check(fi, check.HasLen, expectLen)
	}
}

func (s *CollectionFSSuite) TestOpenFileFlags(c *check.C) {
	fs, err := (&Collection{}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)

	f, err := fs.OpenFile("missing", os.O_WRONLY, 0)
	c.Check(f, check.IsNil)
	c.Check(err, check.ErrorMatches, `file does not exist`)

	f, err = fs.OpenFile("new", os.O_CREATE|os.O_RDONLY, 0)
	c.Assert(err, check.IsNil)
	defer f.Close()
	n, err := f.Write([]byte{1, 2, 3})
	c.Check(n, check.Equals, 0)
	c.Check(err, check.ErrorMatches, `read-only file`)
	n, err = f.Read(make([]byte, 1))
	c.Check(n, check.Equals, 0)
	c.Check(err, check.Equals, io.EOF)
	f, err = fs.OpenFile("new", os.O_RDWR, 0)
	c.Assert(err, check.IsNil)
	defer f.Close()
	_, err = f.Write([]byte{4, 5, 6})
	c.Check(err, check.IsNil)
	fi, err := f.Stat()
	c.Assert(err, check.IsNil)
	c.Check(fi.Size(), check.Equals, int64(3))

	f, err = fs.OpenFile("new", os.O_TRUNC|os.O_RDWR, 0)
	c.Assert(err, check.IsNil)
	defer f.Close()
	pos, err := f.Seek(0, io.SeekEnd)
	c.Check(pos, check.Equals, int64(0))
	c.Check(err, check.IsNil)
	fi, err = f.Stat()
	c.Assert(err, check.IsNil)
	c.Check(fi.Size(), check.Equals, int64(0))
	fs.Remove("new")

	buf := make([]byte, 64)
	f, err = fs.OpenFile("append", os.O_EXCL|os.O_CREATE|os.O_RDWR|os.O_APPEND, 0)
	c.Assert(err, check.IsNil)
	f.Write([]byte{1, 2, 3})
	f.Seek(0, io.SeekStart)
	n, _ = f.Read(buf[:1])
	c.Check(n, check.Equals, 1)
	c.Check(buf[:1], check.DeepEquals, []byte{1})
	pos, err = f.Seek(0, io.SeekCurrent)
	c.Assert(err, check.IsNil)
	c.Check(pos, check.Equals, int64(1))
	f.Write([]byte{4, 5, 6})
	pos, err = f.Seek(0, io.SeekCurrent)
	c.Assert(err, check.IsNil)
	c.Check(pos, check.Equals, int64(6))
	f.Seek(0, io.SeekStart)
	n, err = f.Read(buf)
	c.Check(buf[:n], check.DeepEquals, []byte{1, 2, 3, 4, 5, 6})
	c.Check(err, check.Equals, io.EOF)
	f.Close()

	f, err = fs.OpenFile("append", os.O_RDWR|os.O_APPEND, 0)
	c.Assert(err, check.IsNil)
	pos, err = f.Seek(0, io.SeekCurrent)
	c.Check(pos, check.Equals, int64(0))
	c.Check(err, check.IsNil)
	f.Read(buf[:3])
	pos, _ = f.Seek(0, io.SeekCurrent)
	c.Check(pos, check.Equals, int64(3))
	f.Write([]byte{7, 8, 9})
	pos, err = f.Seek(0, io.SeekCurrent)
	c.Check(err, check.IsNil)
	c.Check(pos, check.Equals, int64(9))
	f.Close()

	f, err = fs.OpenFile("wronly", os.O_CREATE|os.O_WRONLY, 0)
	c.Assert(err, check.IsNil)
	n, err = f.Write([]byte{3, 2, 1})
	c.Check(n, check.Equals, 3)
	c.Check(err, check.IsNil)
	pos, _ = f.Seek(0, io.SeekCurrent)
	c.Check(pos, check.Equals, int64(3))
	pos, _ = f.Seek(0, io.SeekStart)
	c.Check(pos, check.Equals, int64(0))
	n, err = f.Read(buf)
	c.Check(n, check.Equals, 0)
	c.Check(err, check.ErrorMatches, `.*O_WRONLY.*`)
	f, err = fs.OpenFile("wronly", os.O_RDONLY, 0)
	c.Assert(err, check.IsNil)
	n, _ = f.Read(buf)
	c.Check(buf[:n], check.DeepEquals, []byte{3, 2, 1})

	f, err = fs.OpenFile("unsupported", os.O_CREATE|os.O_SYNC, 0)
	c.Check(f, check.IsNil)
	c.Check(err, check.NotNil)

	f, err = fs.OpenFile("append", os.O_RDWR|os.O_WRONLY, 0)
	c.Check(f, check.IsNil)
	c.Check(err, check.ErrorMatches, `invalid flag.*`)
}

func (s *CollectionFSSuite) TestFlushFullBlocksWritingLongFile(c *check.C) {
	defer func(cw, mbs int) {
		concurrentWriters = cw
		maxBlockSize = mbs
	}(concurrentWriters, maxBlockSize)
	concurrentWriters = 2
	maxBlockSize = 1024

	proceed := make(chan struct{})
	var started, concurrent int32
	blk2done := false
	s.kc.onWrite = func([]byte) error {
		atomic.AddInt32(&concurrent, 1)
		switch atomic.AddInt32(&started, 1) {
		case 1:
			// Wait until block 2 starts and finishes, and block 3 starts
			select {
			case <-proceed:
				c.Check(blk2done, check.Equals, true)
			case <-time.After(time.Second):
				c.Error("timed out")
			}
		case 2:
			time.Sleep(time.Millisecond)
			blk2done = true
		case 3:
			close(proceed)
		default:
			time.Sleep(time.Millisecond)
		}
		c.Check(atomic.AddInt32(&concurrent, -1) < int32(concurrentWriters), check.Equals, true)
		return nil
	}

	fs, err := (&Collection{}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)
	f, err := fs.OpenFile("50K", os.O_WRONLY|os.O_CREATE, 0)
	c.Assert(err, check.IsNil)
	defer f.Close()

	data := make([]byte, 500)
	rand.Read(data)

	for i := 0; i < 100; i++ {
		n, err := f.Write(data)
		c.Assert(n, check.Equals, len(data))
		c.Assert(err, check.IsNil)
	}

	currentMemExtents := func() (memExtents []int) {
		for idx, e := range f.(*filehandle).inode.(*filenode).segments {
			switch e.(type) {
			case *memSegment:
				memExtents = append(memExtents, idx)
			}
		}
		return
	}
	f.(*filehandle).inode.(*filenode).waitPrune()
	c.Check(currentMemExtents(), check.HasLen, 1)

	m, err := fs.MarshalManifest(".")
	c.Check(m, check.Matches, `[^:]* 0:50000:50K\n`)
	c.Check(err, check.IsNil)
	c.Check(currentMemExtents(), check.HasLen, 0)
}

// Ensure blocks get flushed to disk if a lot of data is written to
// small files/directories without calling sync().
//
// Write four 512KiB files into each of 256 top-level dirs (total
// 512MiB), calling Flush() every 8 dirs. Ensure memory usage never
// exceeds 24MiB (4 concurrentWriters * 2MiB + 8 unflushed dirs *
// 2MiB).
func (s *CollectionFSSuite) TestFlushAll(c *check.C) {
	fs, err := (&Collection{}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)

	s.kc.onWrite = func([]byte) error {
		// discard flushed data -- otherwise the stub will use
		// unlimited memory
		time.Sleep(time.Millisecond)
		s.kc.Lock()
		defer s.kc.Unlock()
		s.kc.blocks = map[string][]byte{}
		return nil
	}
	for i := 0; i < 256; i++ {
		buf := bytes.NewBuffer(make([]byte, 524288))
		fmt.Fprintf(buf, "test file in dir%d", i)

		dir := fmt.Sprintf("dir%d", i)
		fs.Mkdir(dir, 0755)
		for j := 0; j < 2; j++ {
			f, err := fs.OpenFile(fmt.Sprintf("%s/file%d", dir, j), os.O_WRONLY|os.O_CREATE, 0)
			c.Assert(err, check.IsNil)
			defer f.Close()
			_, err = io.Copy(f, buf)
			c.Assert(err, check.IsNil)
		}

		if i%8 == 0 {
			fs.Flush("", true)
		}

		size := fs.MemorySize()
		if !c.Check(size <= 1<<24, check.Equals, true) {
			c.Logf("at dir%d fs.MemorySize()=%d", i, size)
			return
		}
	}
}

// Ensure short blocks at the end of a stream don't get flushed by
// Flush(false).
//
// Write 67x 1MiB files to each of 8 dirs, and check that 8 full 64MiB
// blocks have been flushed while 8x 3MiB is still buffered in memory.
func (s *CollectionFSSuite) TestFlushFullBlocksOnly(c *check.C) {
	fs, err := (&Collection{}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)

	var flushed int64
	s.kc.onWrite = func(p []byte) error {
		atomic.AddInt64(&flushed, int64(len(p)))
		return nil
	}

	nDirs := int64(8)
	nFiles := int64(67)
	megabyte := make([]byte, 1<<20)
	for i := int64(0); i < nDirs; i++ {
		dir := fmt.Sprintf("dir%d", i)
		fs.Mkdir(dir, 0755)
		for j := int64(0); j < nFiles; j++ {
			f, err := fs.OpenFile(fmt.Sprintf("%s/file%d", dir, j), os.O_WRONLY|os.O_CREATE, 0)
			c.Assert(err, check.IsNil)
			defer f.Close()
			_, err = f.Write(megabyte)
			c.Assert(err, check.IsNil)
		}
	}
	inodebytes := int64((nDirs*(nFiles+1) + 1) * 64)
	c.Check(fs.MemorySize(), check.Equals, nDirs*nFiles*(1<<20+64)+inodebytes)
	c.Check(flushed, check.Equals, int64(0))

	waitForFlush := func(expectUnflushed, expectFlushed int64) {
		for deadline := time.Now().Add(5 * time.Second); fs.MemorySize() > expectUnflushed && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		}
		c.Check(fs.MemorySize(), check.Equals, expectUnflushed)
		c.Check(flushed, check.Equals, expectFlushed)
	}

	// Nothing flushed yet
	waitForFlush(nDirs*nFiles*(1<<20+64)+inodebytes, 0)

	// Flushing a non-empty dir "/" is non-recursive and there are
	// no top-level files, so this has no effect
	fs.Flush("/", false)
	waitForFlush(nDirs*nFiles*(1<<20+64)+inodebytes, 0)

	// Flush the full block in dir0
	fs.Flush("dir0", false)
	bigloclen := int64(32 + 9 + 51 + 64) // md5 + "+" + "67xxxxxx" + "+Axxxxxx..." + 64 (see (storedSegment)memorySize)
	waitForFlush((nDirs*nFiles-64)*(1<<20+64)+inodebytes+bigloclen*64, 64<<20)

	err = fs.Flush("dir-does-not-exist", false)
	c.Check(err, check.NotNil)

	// Flush full blocks in all dirs
	fs.Flush("", false)
	waitForFlush(nDirs*3*(1<<20+64)+inodebytes+bigloclen*64*nDirs, nDirs*64<<20)

	// Flush non-full blocks, too
	fs.Flush("", true)
	smallloclen := int64(32 + 8 + 51 + 64) // md5 + "+" + "3xxxxxx" + "+Axxxxxx..." + 64 (see (storedSegment)memorySize)
	waitForFlush(inodebytes+bigloclen*64*nDirs+smallloclen*3*nDirs, nDirs*67<<20)
}

// Even when writing lots of files/dirs from different goroutines, as
// long as Flush(dir,false) is called after writing each file,
// unflushed data should be limited to one full block per
// concurrentWriter, plus one nearly-full block at the end of each
// dir/stream.
func (s *CollectionFSSuite) TestMaxUnflushed(c *check.C) {
	nDirs := int64(8)
	maxUnflushed := (int64(concurrentWriters) + nDirs) << 26

	fs, err := (&Collection{}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)

	release := make(chan struct{})
	timeout := make(chan struct{})
	time.AfterFunc(10*time.Second, func() { close(timeout) })
	var putCount, concurrency int64
	var unflushed int64
	s.kc.onWrite = func(p []byte) error {
		defer atomic.AddInt64(&unflushed, -int64(len(p)))
		cur := atomic.AddInt64(&concurrency, 1)
		defer atomic.AddInt64(&concurrency, -1)
		pc := atomic.AddInt64(&putCount, 1)
		if pc < int64(concurrentWriters) {
			// Block until we reach concurrentWriters, to
			// make sure we're really accepting concurrent
			// writes.
			select {
			case <-release:
			case <-timeout:
				c.Error("timeout")
			}
		} else if pc == int64(concurrentWriters) {
			// Unblock the first N-1 PUT reqs.
			close(release)
		}
		c.Assert(cur <= int64(concurrentWriters), check.Equals, true)
		c.Assert(atomic.LoadInt64(&unflushed) <= maxUnflushed, check.Equals, true)
		return nil
	}

	var owg sync.WaitGroup
	megabyte := make([]byte, 1<<20)
	for i := int64(0); i < nDirs; i++ {
		dir := fmt.Sprintf("dir%d", i)
		fs.Mkdir(dir, 0755)
		owg.Add(1)
		go func() {
			defer owg.Done()
			defer fs.Flush(dir, true)
			var iwg sync.WaitGroup
			defer iwg.Wait()
			for j := 0; j < 67; j++ {
				iwg.Add(1)
				go func(j int) {
					defer iwg.Done()
					f, err := fs.OpenFile(fmt.Sprintf("%s/file%d", dir, j), os.O_WRONLY|os.O_CREATE, 0)
					c.Assert(err, check.IsNil)
					defer f.Close()
					n, err := f.Write(megabyte)
					c.Assert(err, check.IsNil)
					atomic.AddInt64(&unflushed, int64(n))
					fs.Flush(dir, false)
				}(j)
			}
		}()
	}
	owg.Wait()
	fs.Flush("", true)
}

func (s *CollectionFSSuite) TestFlushStress(c *check.C) {
	done := false
	defer func() { done = true }()
	time.AfterFunc(10*time.Second, func() {
		if !done {
			pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
			panic("timeout")
		}
	})

	wrote := 0
	s.kc.onWrite = func(p []byte) error {
		s.kc.Lock()
		s.kc.blocks = map[string][]byte{}
		wrote++
		defer c.Logf("wrote block %d, %d bytes", wrote, len(p))
		s.kc.Unlock()
		time.Sleep(20 * time.Millisecond)
		return nil
	}

	fs, err := (&Collection{}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)

	data := make([]byte, 1<<20)
	for i := 0; i < 3; i++ {
		dir := fmt.Sprintf("dir%d", i)
		fs.Mkdir(dir, 0755)
		for j := 0; j < 200; j++ {
			data[0] = byte(j)
			f, err := fs.OpenFile(fmt.Sprintf("%s/file%d", dir, j), os.O_WRONLY|os.O_CREATE, 0)
			c.Assert(err, check.IsNil)
			_, err = f.Write(data)
			c.Assert(err, check.IsNil)
			f.Close()
			fs.Flush(dir, false)
		}
		_, err := fs.MarshalManifest(".")
		c.Check(err, check.IsNil)
	}
}

func (s *CollectionFSSuite) TestFlushShort(c *check.C) {
	s.kc.onWrite = func([]byte) error {
		s.kc.Lock()
		s.kc.blocks = map[string][]byte{}
		s.kc.Unlock()
		return nil
	}
	fs, err := (&Collection{}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)
	for _, blocksize := range []int{8, 1000000} {
		dir := fmt.Sprintf("dir%d", blocksize)
		err = fs.Mkdir(dir, 0755)
		c.Assert(err, check.IsNil)
		data := make([]byte, blocksize)
		for i := 0; i < 100; i++ {
			f, err := fs.OpenFile(fmt.Sprintf("%s/file%d", dir, i), os.O_WRONLY|os.O_CREATE, 0)
			c.Assert(err, check.IsNil)
			_, err = f.Write(data)
			c.Assert(err, check.IsNil)
			f.Close()
			fs.Flush(dir, false)
		}
		fs.Flush(dir, true)
		_, err := fs.MarshalManifest(".")
		c.Check(err, check.IsNil)
	}
}

func (s *CollectionFSSuite) TestBrokenManifests(c *check.C) {
	for _, txt := range []string{
		"\n",
		".\n",
		". \n",
		". d41d8cd98f00b204e9800998ecf8427e+0\n",
		". d41d8cd98f00b204e9800998ecf8427e+0 \n",
		". 0:0:foo\n",
		".  0:0:foo\n",
		". 0:0:foo 0:0:bar\n",
		". d41d8cd98f00b204e9800998ecf8427e 0:0:foo\n",
		". d41d8cd98f00b204e9800998ecf8427e+0 :0:0:foo\n",
		". d41d8cd98f00b204e9800998ecf8427e+0 foo:0:foo\n",
		". d41d8cd98f00b204e9800998ecf8427e+0 0:foo:foo\n",
		". d41d8cd98f00b204e9800998ecf8427e+1 0:1:foo 1:1:bar\n",
		". d41d8cd98f00b204e9800998ecf8427e+1 0:1:\\056\n",
		". d41d8cd98f00b204e9800998ecf8427e+1 0:1:\\056\\057\\056\n",
		". d41d8cd98f00b204e9800998ecf8427e+1 0:1:.\n",
		". d41d8cd98f00b204e9800998ecf8427e+1 0:1:..\n",
		". d41d8cd98f00b204e9800998ecf8427e+0 0:0:..\n",
		". d41d8cd98f00b204e9800998ecf8427e+0 0:0:foo/..\n",
		". d41d8cd98f00b204e9800998ecf8427e+1 0:0:foo\n./foo d41d8cd98f00b204e9800998ecf8427e+1 0:0:bar\n",
		"./foo d41d8cd98f00b204e9800998ecf8427e+1 0:0:bar\n. d41d8cd98f00b204e9800998ecf8427e+1 0:0:foo\n",
	} {
		c.Logf("<-%q", txt)
		fs, err := (&Collection{ManifestText: txt}).FileSystem(s.client, s.kc)
		c.Check(fs, check.IsNil)
		c.Logf("-> %s", err)
		c.Check(err, check.NotNil)
	}
}

func (s *CollectionFSSuite) TestEdgeCaseManifests(c *check.C) {
	for _, txt := range []string{
		"",
		". d41d8cd98f00b204e9800998ecf8427e+0 0:0:foo\n",
		". d41d8cd98f00b204e9800998ecf8427e+0 0:0:...\n",
		". d41d8cd98f00b204e9800998ecf8427e+0 0:0:. 0:0:. 0:0:\\056 0:0:\\056\n",
		". d41d8cd98f00b204e9800998ecf8427e+0 0:0:foo/. 0:0:. 0:0:foo\\057bar\\057\\056\n",
		". d41d8cd98f00b204e9800998ecf8427e+0 0:0:foo 0:0:foo 0:0:bar\n",
		". d41d8cd98f00b204e9800998ecf8427e+0 0:0:foo/bar\n./foo d41d8cd98f00b204e9800998ecf8427e+0 0:0:bar\n",
	} {
		c.Logf("<-%q", txt)
		fs, err := (&Collection{ManifestText: txt}).FileSystem(s.client, s.kc)
		c.Check(err, check.IsNil)
		c.Check(fs, check.NotNil)
	}
}

var fakeLocator = func() []string {
	locs := make([]string, 10)
	for i := range locs {
		locs[i] = fmt.Sprintf("%x+%d", md5.Sum(make([]byte, i)), i)
		if i%2 == 1 {
			locs[i] += "+Awhatever+Zotherhints"
		}
	}
	return locs
}()

func (s *CollectionFSSuite) TestReplaceSegments_HappyPath(c *check.C) {
	fs, err := (&Collection{
		ManifestText: ". " + fakeLocator[1] + " " + fakeLocator[2] + " 0:3:file3\n",
	}).FileSystem(nil, &keepClientStub{})
	c.Assert(err, check.IsNil)
	changed, err := fs.ReplaceSegments(map[BlockSegment]BlockSegment{
		BlockSegment{fakeLocator[1], 0, 1}: BlockSegment{fakeLocator[3], 0, 1},
		BlockSegment{fakeLocator[2], 0, 2}: BlockSegment{fakeLocator[3], 1, 2},
	})
	c.Check(changed, check.Equals, true)
	c.Check(err, check.IsNil)
	mtxt, err := fs.MarshalManifest(".")
	c.Check(err, check.IsNil)
	c.Check(mtxt, check.Equals, ". "+fakeLocator[3]+" 0:3:file3\n")
}

func (s *CollectionFSSuite) TestReplaceSegments_InvalidOffset(c *check.C) {
	origtxt := ". " + fakeLocator[1] + " " + fakeLocator[2] + " 0:3:file3\n"
	fs, err := (&Collection{
		ManifestText: origtxt,
	}).FileSystem(nil, &keepClientStub{})
	c.Assert(err, check.IsNil)
	changed, err := fs.ReplaceSegments(map[BlockSegment]BlockSegment{
		BlockSegment{fakeLocator[1], 0, 1}: BlockSegment{fakeLocator[3], 0, 1},
		BlockSegment{fakeLocator[2], 0, 2}: BlockSegment{fakeLocator[3], 2, 2},
	})
	c.Check(changed, check.Equals, false)
	c.Check(err, check.ErrorMatches, `invalid replacement: offset 2 \+ length 2 > block size 3`)
	mtxt, err := fs.MarshalManifest(".")
	c.Check(err, check.IsNil)
	c.Check(mtxt, check.Equals, origtxt)
}

func (s *CollectionFSSuite) TestReplaceSegments_LengthMismatch(c *check.C) {
	origtxt := ". " + fakeLocator[1] + " " + fakeLocator[2] + " 0:3:file3\n"
	fs, err := (&Collection{
		ManifestText: origtxt,
	}).FileSystem(nil, &keepClientStub{})
	c.Assert(err, check.IsNil)
	changed, err := fs.ReplaceSegments(map[BlockSegment]BlockSegment{
		BlockSegment{fakeLocator[1], 0, 1}: BlockSegment{fakeLocator[3], 0, 1},
		BlockSegment{fakeLocator[2], 0, 2}: BlockSegment{fakeLocator[3], 0, 3},
	})
	c.Check(changed, check.Equals, false)
	c.Check(err, check.ErrorMatches, `mismatched length: replacing segment length 2 with segment length 3`)
	mtxt, err := fs.MarshalManifest(".")
	c.Check(err, check.IsNil)
	c.Check(mtxt, check.Equals, origtxt)
}

func (s *CollectionFSSuite) TestReplaceSegments_SkipUnreferenced(c *check.C) {
	fs, err := (&Collection{
		ManifestText: ". " + fakeLocator[1] + " " + fakeLocator[2] + " " + fakeLocator[3] + " 0:6:file6\n",
	}).FileSystem(nil, &keepClientStub{})
	c.Assert(err, check.IsNil)
	changed, err := fs.ReplaceSegments(map[BlockSegment]BlockSegment{
		BlockSegment{fakeLocator[1], 0, 1}: BlockSegment{fakeLocator[4], 0, 1}, // skipped because [5] unref
		BlockSegment{fakeLocator[2], 0, 2}: BlockSegment{fakeLocator[4], 1, 2}, // skipped because [5] unref
		BlockSegment{fakeLocator[5], 0, 2}: BlockSegment{fakeLocator[4], 1, 2}, // [5] unreferenced in orig manifest
		BlockSegment{fakeLocator[3], 0, 3}: BlockSegment{fakeLocator[6], 3, 3}, // applied
	})
	c.Check(changed, check.Equals, true)
	c.Check(err, check.IsNil)
	mtxt, err := fs.MarshalManifest(".")
	c.Check(err, check.IsNil)
	c.Check(mtxt, check.Equals, ". "+fakeLocator[1]+" "+fakeLocator[2]+" "+fakeLocator[6]+" 0:3:file6 6:3:file6\n")
}

func (s *CollectionFSSuite) TestReplaceSegments_SkipIncompleteSegment(c *check.C) {
	origtxt := ". " + fakeLocator[2] + " " + fakeLocator[3] + " 0:5:file5\n"
	fs, err := (&Collection{
		ManifestText: origtxt,
	}).FileSystem(nil, &keepClientStub{})
	c.Assert(err, check.IsNil)
	changed, err := fs.ReplaceSegments(map[BlockSegment]BlockSegment{
		BlockSegment{fakeLocator[2], 0, 1}: BlockSegment{fakeLocator[4], 0, 1}, // length=1 does not match the length=2 segment
	})
	c.Check(changed, check.Equals, false)
	c.Check(err, check.IsNil)
	mtxt, err := fs.MarshalManifest(".")
	c.Check(err, check.IsNil)
	c.Check(mtxt, check.Equals, origtxt)
}

func (s *CollectionFSSuite) testPlanRepack(c *check.C, opts RepackOptions, manifest string, expectPlan [][]storedSegment) {
	fs, err := (&Collection{ManifestText: manifest}).FileSystem(nil, s.kc)
	c.Assert(err, check.IsNil)
	cfs := fs.(*collectionFileSystem)
	repl, err := cfs.planRepack(context.Background(), opts, cfs.root.(*dirnode))
	c.Assert(err, check.IsNil)

	// we always expect kc==cfs, so we fill this in instead of
	// requiring each test case to repeat it
	for _, pp := range expectPlan {
		for i := range pp {
			pp[i].kc = cfs
		}
	}
	c.Check(repl, check.DeepEquals, expectPlan)
}

func (s *CollectionFSSuite) TestPlanRepack_2x32M(c *check.C) {
	s.testPlanRepack(c,
		RepackOptions{Full: true},
		". aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+32000000 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb+32000000 0:64000000:file\n",
		[][]storedSegment{
			{
				{locator: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+32000000", size: 32000000, length: 32000000, offset: 0},
				{locator: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb+32000000", size: 32000000, length: 32000000, offset: 0},
			},
		})
}

func (s *CollectionFSSuite) TestPlanRepack_2x32M_Cached(c *check.C) {
	s.kc.cached = map[string]bool{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": true,
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb": true,
	}
	s.testPlanRepack(c,
		RepackOptions{Full: true, CachedOnly: true},
		". aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+32000000 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb+32000000 0:64000000:file\n",
		[][]storedSegment{
			{
				{locator: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+32000000", size: 32000000, length: 32000000, offset: 0},
				{locator: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb+32000000", size: 32000000, length: 32000000, offset: 0},
			},
		})
}

func (s *CollectionFSSuite) TestPlanRepack_2x32M_OneCached(c *check.C) {
	s.kc.cached = map[string]bool{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": true,
	}
	s.testPlanRepack(c,
		RepackOptions{Full: true, CachedOnly: true},
		". aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+32000000 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb+32000000 0:64000000:file\n",
		nil)
}

func (s *CollectionFSSuite) TestPlanRepack_3x32M_TwoCached(c *check.C) {
	s.kc.cached = map[string]bool{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": true,
		"cccccccccccccccccccccccccccccccc": true,
	}
	s.testPlanRepack(c,
		RepackOptions{Full: true, CachedOnly: true},
		". aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+32000000 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb+32000000 cccccccccccccccccccccccccccccccc+32000000 0:96000000:file\n",
		[][]storedSegment{
			{
				{locator: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+32000000", size: 32000000, length: 32000000, offset: 0},
				{locator: "cccccccccccccccccccccccccccccccc+32000000", size: 32000000, length: 32000000, offset: 0},
			},
		})
}

func (s *CollectionFSSuite) TestPlanRepack_2x32Mi(c *check.C) {
	s.testPlanRepack(c,
		RepackOptions{Full: true},
		". aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+33554432 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb+33554432 0:67108864:file\n",
		nil)
}

func (s *CollectionFSSuite) TestPlanRepack_2x32MiMinus1(c *check.C) {
	s.testPlanRepack(c,
		RepackOptions{Full: true},
		". aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+33554431 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb+33554431 0:67108862:file\n",
		[][]storedSegment{
			{
				{locator: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+33554431", size: 33554431, length: 33554431, offset: 0},
				{locator: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb+33554431", size: 33554431, length: 33554431, offset: 0},
			},
		})
}

func (s *CollectionFSSuite) TestPlanRepack_3x32M(c *check.C) {
	s.testPlanRepack(c,
		RepackOptions{Full: true},
		". aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+32000000 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb+32000000 cccccccccccccccccccccccccccccccc+32000000 0:96000000:file\n",
		[][]storedSegment{
			{
				{locator: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+32000000", size: 32000000, length: 32000000, offset: 0},
				{locator: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb+32000000", size: 32000000, length: 32000000, offset: 0},
			},
		})
}

func (s *CollectionFSSuite) TestPlanRepack_3x42M(c *check.C) {
	// Each block is more than half full, so do nothing.
	s.testPlanRepack(c,
		RepackOptions{Full: true},
		". aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+42000000 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb+42000000 cccccccccccccccccccccccccccccccc+42000000 0:126000000:file\n",
		nil)
}

func (s *CollectionFSSuite) TestPlanRepack_Premature(c *check.C) {
	// Repacking would reduce to one block, but it would still be
	// too short to be worthwhile, so do nothing.
	s.testPlanRepack(c,
		RepackOptions{Full: true},
		". aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+123 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb+123 cccccccccccccccccccccccccccccccc+123 0:369:file\n",
		nil)
}

func (s *CollectionFSSuite) TestPlanRepack_4x22M_NonAdjacent(c *check.C) {
	// Repack the first three 22M blocks into one 66M block.
	// Don't touch the 44M blocks or the final 22M block.
	s.testPlanRepack(c,
		RepackOptions{Full: true},
		". aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+22000000 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb+44000000 cccccccccccccccccccccccccccccccc+22000000 dddddddddddddddddddddddddddddddd+44000000 eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee+22000000 ffffffffffffffffffffffffffffffff+44000000 00000000000000000000000000000000+22000000 0:220000000:file\n",
		[][]storedSegment{
			{
				{locator: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+22000000", size: 22000000, length: 22000000, offset: 0},
				{locator: "cccccccccccccccccccccccccccccccc+22000000", size: 22000000, length: 22000000, offset: 0},
				{locator: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee+22000000", size: 22000000, length: 22000000, offset: 0},
			},
		})
}

func (s *CollectionFSSuite) TestPlanRepack_2x22M_DuplicateBlock(c *check.C) {
	// Repack a+b+c, not a+b+a.
	s.testPlanRepack(c,
		RepackOptions{Full: true},
		". aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+22000000 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb+22000000 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+22000000 0:66000000:file\n"+
			"./dir cccccccccccccccccccccccccccccccc+22000000 0:22000000:file\n",
		[][]storedSegment{
			{
				{locator: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+22000000", size: 22000000, length: 22000000, offset: 0},
				{locator: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb+22000000", size: 22000000, length: 22000000, offset: 0},
				{locator: "cccccccccccccccccccccccccccccccc+22000000", size: 22000000, length: 22000000, offset: 0},
			},
		})
}

func (s *CollectionFSSuite) TestPlanRepack_2x22M_DuplicateBlock_TooShort(c *check.C) {
	// Repacking a+b would not meet the 32MiB threshold.
	s.testPlanRepack(c,
		RepackOptions{Full: true},
		". aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+22000000 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb+1 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+22000000 0:44000001:file\n",
		nil)
}

func (s *CollectionFSSuite) TestPlanRepack_SiblingsTogether(c *check.C) {
	// Pack sibling files' ("a" and "c") segments together before
	// other subdirs ("b/b"), even though subdir "b" sorts between
	// "a" and "c".
	s.testPlanRepack(c,
		RepackOptions{Full: true},
		". aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+15000000 cccccccccccccccccccccccccccccccc+15000000 0:15000000:a 15000000:15000000:c\n"+
			"./b bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb+15000000 0:15000000:b\n",
		[][]storedSegment{
			{
				{locator: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa+15000000", size: 15000000, length: 15000000, offset: 0},
				{locator: "cccccccccccccccccccccccccccccccc+15000000", size: 15000000, length: 15000000, offset: 0},
				{locator: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb+15000000", size: 15000000, length: 15000000, offset: 0},
			},
		})
}

func (s *CollectionFSSuite) TestRepackData(c *check.C) {
	fs, err := (&Collection{}).FileSystem(nil, s.kc)
	c.Assert(err, check.IsNil)
	cfs := fs.(*collectionFileSystem)

	testBlockWritten := make(map[int]string)
	// testSegment(N) returns an N-byte segment of a block
	// containing repeated byte N%256.  The segment's offset
	// within the block is N/1000000 (*).  The block also has
	// N/1000000 null bytes following the segment(*).
	//
	// If N=404, the block is not readable.
	//
	// (*) ...unless that would result in an oversize block.
	testSegment := func(testSegmentNum int) storedSegment {
		length := testSegmentNum
		offset := testSegmentNum / 1000000
		if offset+length > maxBlockSize {
			offset = 0
		}
		size := testSegmentNum + offset
		if size+offset <= maxBlockSize {
			size += offset
		}
		if _, stored := testBlockWritten[testSegmentNum]; !stored {
			data := make([]byte, size)
			for b := range data[offset : offset+length] {
				data[b] = byte(testSegmentNum & 0xff)
			}
			resp, err := s.kc.BlockWrite(context.Background(), BlockWriteOptions{Data: data})
			c.Assert(err, check.IsNil)
			testBlockWritten[testSegmentNum] = resp.Locator
			if testSegmentNum == 404 {
				delete(s.kc.blocks, resp.Locator[:32])
			}
		}
		return storedSegment{
			kc:      cfs,
			locator: testBlockWritten[testSegmentNum],
			size:    size,
			length:  length,
			offset:  offset,
		}
	}
	for trialIndex, trial := range []struct {
		label string
		// "input" here has the same shape as repackData's
		// [][]storedSegment argument, but uses int N has
		// shorthand for testSegment(N).
		input              [][]int
		onWrite            func([]byte) error
		expectRepackedLen  int
		expectErrorMatches string
	}{
		{
			label:             "one {3 blocks to 1} merge",
			input:             [][]int{{1, 2, 3}},
			expectRepackedLen: 3,
		},
		{
			label:             "two {3 blocks to 1} merges",
			input:             [][]int{{1, 2, 3}, {4, 5, 6}},
			expectRepackedLen: 6,
		},
		{
			label:             "merge two {3 blocks to 1} merges",
			input:             [][]int{{1, 2, 3}, {4, 5, 6}},
			expectRepackedLen: 6,
		},
		{
			label:             "no-op",
			input:             nil,
			expectRepackedLen: 0,
		},
		{
			label:             "merge 3 blocks plus a zero-length segment -- not expected to be used, but should work",
			input:             [][]int{{1, 2, 0, 3}},
			expectRepackedLen: 4,
		},
		{
			label:             "merge a single segment -- not expected to be used, but should work",
			input:             [][]int{{12345}},
			expectRepackedLen: 1,
		},
		{
			label:             "merge a single empty segment -- not expected to be used, but should work",
			input:             [][]int{{0}},
			expectRepackedLen: 1,
		},
		{
			label:             "merge zero segments -- not expected to be used, but should work",
			input:             [][]int{{}},
			expectRepackedLen: 0,
		},
		{
			label:             "merge same orig segment into two different replacements -- not expected to be used, but should work",
			input:             [][]int{{1, 22, 3}, {4, 22, 6}},
			expectRepackedLen: 5,
		},
		{
			label:             "identical merges -- not expected to be used, but should work",
			input:             [][]int{{11, 22, 33}, {11, 22, 33}},
			expectRepackedLen: 3,
		},
		{
			label:              "read error on first segment",
			input:              [][]int{{404, 2, 3}},
			expectRepackedLen:  0,
			expectErrorMatches: "404 block not found",
		},
		{
			label:              "read error on second segment",
			input:              [][]int{{1, 404, 3}},
			expectErrorMatches: "404 block not found",
		},
		{
			label:              "read error on last segment",
			input:              [][]int{{1, 2, 404}},
			expectErrorMatches: "404 block not found",
		},
		{
			label:              "merge does not fit in one block",
			input:              [][]int{{50000000, 20000000}},
			expectErrorMatches: "combined length 70000000 would exceed maximum block size 67108864",
		},
		{
			label:              "write error",
			input:              [][]int{{1, 2, 3}},
			onWrite:            func(p []byte) error { return errors.New("stub write error") },
			expectErrorMatches: "stub write error",
		},
	} {
		c.Logf("trial %d: %s", trialIndex, trial.label)
		var input [][]storedSegment
		for _, seglist := range trial.input {
			var segments []storedSegment
			for _, segnum := range seglist {
				segments = append(segments, testSegment(segnum))
			}
			input = append(input, segments)
		}
		s.kc.onWrite = trial.onWrite
		repacked, err := cfs.repackData(context.Background(), input)
		if trial.expectErrorMatches != "" {
			c.Check(err, check.ErrorMatches, trial.expectErrorMatches)
			continue
		}
		c.Assert(err, check.IsNil)
		c.Check(repacked, check.HasLen, trial.expectRepackedLen)
		for _, origSegments := range input {
			replLocator := ""
			for _, origSegment := range origSegments {
				origBlock := BlockSegment{
					Locator: stripAllHints(origSegment.locator),
					Length:  origSegment.length,
					Offset:  origSegment.offset,
				}
				buf := make([]byte, origSegment.size)
				n, err := cfs.ReadAt(repacked[origBlock].Locator, buf, repacked[origBlock].Offset)
				c.Assert(err, check.IsNil)
				c.Check(n, check.Equals, len(buf))
				expectContent := byte(origSegment.length & 0xff)
				for segoffset, b := range buf {
					if b != expectContent {
						c.Errorf("content mismatch: origSegment.locator %s -> replLocator %s offset %d: byte %d is %d, expected %d", origSegment.locator, replLocator, repacked[origBlock].Offset, segoffset, b, expectContent)
						break
					}
				}
			}
		}
	}
}

type dataToWrite struct {
	path string
	data func() []byte
}

func dataToWrite_SourceTree(c *check.C, maxfiles int) (writes []dataToWrite) {
	gitdir, err := filepath.Abs("../../..")
	c.Assert(err, check.IsNil)
	infs := os.DirFS(gitdir)
	buf, err := exec.Command("git", "-C", gitdir, "ls-files").CombinedOutput()
	c.Assert(err, check.IsNil, check.Commentf("%s", buf))
	for _, path := range bytes.Split(buf, []byte("\n")) {
		path := string(path)
		if path == "" ||
			strings.HasPrefix(path, "tools/arvbox/lib/arvbox/docker/service") &&
				strings.HasSuffix(path, "/run") {
			// dangling symlink
			continue
		}
		fi, err := fs.Stat(infs, path)
		c.Assert(err, check.IsNil)
		if fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			continue
		}
		writes = append(writes, dataToWrite{
			path: path,
			data: func() []byte {
				data, err := fs.ReadFile(infs, path)
				c.Assert(err, check.IsNil)
				return data
			},
		})
		if len(writes) >= maxfiles {
			break
		}
	}
	return
}

func dataToWrite_ConstantSizeFilesInDirs(c *check.C, ndirs, nfiles, filesize, chunksize int) (writes []dataToWrite) {
	for chunk := 0; chunk == 0 || (chunksize > 0 && chunk < (filesize+chunksize-1)/chunksize); chunk++ {
		for i := 0; i < nfiles; i++ {
			datasize := filesize
			if chunksize > 0 {
				datasize = chunksize
				if remain := filesize - chunk*chunksize; remain < chunksize {
					datasize = remain
				}
			}
			data := make([]byte, datasize)
			copy(data, []byte(fmt.Sprintf("%d chunk %d", i, chunk)))
			writes = append(writes, dataToWrite{
				path: fmt.Sprintf("dir%d/file%d", i*ndirs/nfiles, i),
				data: func() []byte { return data },
			})
		}
	}
	return
}

var enableRepackCharts = os.Getenv("ARVADOS_TEST_REPACK_CHARTS") != ""

func (s *CollectionFSSuite) skipMostRepackCostTests(c *check.C) {
	if !enableRepackCharts {
		c.Skip("Set ARVADOS_TEST_REPACK_CHARTS to run more cost tests and generate data for charts like https://dev.arvados.org/issues/22320#note-14")
	}
}

func (s *CollectionFSSuite) TestRepackCost_SourceTree_Part(c *check.C) {
	s.testRepackCost(c, dataToWrite_SourceTree(c, 500), 40)
}

func (s *CollectionFSSuite) TestRepackCost_SourceTree(c *check.C) {
	s.skipMostRepackCostTests(c)
	s.testRepackCost(c, dataToWrite_SourceTree(c, 99999), 50)
}

func (s *CollectionFSSuite) TestRepackCost_1000x_1M_Files(c *check.C) {
	s.skipMostRepackCostTests(c)
	s.testRepackCost(c, dataToWrite_ConstantSizeFilesInDirs(c, 10, 1000, 1000000, 0), 80)
}

func (s *CollectionFSSuite) TestRepackCost_100x_8M_Files(c *check.C) {
	s.skipMostRepackCostTests(c)
	s.testRepackCost(c, dataToWrite_ConstantSizeFilesInDirs(c, 10, 100, 8000000, 0), 20)
}

func (s *CollectionFSSuite) TestRepackCost_100x_8M_Files_1M_Chunks(c *check.C) {
	s.skipMostRepackCostTests(c)
	s.testRepackCost(c, dataToWrite_ConstantSizeFilesInDirs(c, 10, 100, 8000000, 1000000), 50)
}

func (s *CollectionFSSuite) TestRepackCost_100x_10M_Files_1M_Chunks(c *check.C) {
	s.skipMostRepackCostTests(c)
	s.testRepackCost(c, dataToWrite_ConstantSizeFilesInDirs(c, 10, 100, 10000000, 1000000), 80)
}

func (s *CollectionFSSuite) TestRepackCost_100x_10M_Files(c *check.C) {
	s.skipMostRepackCostTests(c)
	s.testRepackCost(c, dataToWrite_ConstantSizeFilesInDirs(c, 10, 100, 10000000, 0), 100)
}

func (s *CollectionFSSuite) testRepackCost(c *check.C, writes []dataToWrite, maxBlocks int) {
	s.kc.blocks = make(map[string][]byte)
	testfs, err := (&Collection{}).FileSystem(nil, s.kc)
	c.Assert(err, check.IsNil)
	cfs := testfs.(*collectionFileSystem)
	dirsCreated := make(map[string]bool)
	bytesContent := 0
	bytesWritten := func() (n int) {
		s.kc.Lock()
		defer s.kc.Unlock()
		for _, data := range s.kc.blocks {
			n += len(data)
		}
		return
	}
	blocksInManifest := func() int {
		blocks := make(map[string]bool)
		cfs.fileSystem.root.(*dirnode).walkSegments(func(s segment) segment {
			blocks[s.(storedSegment).blockSegment().StripAllHints().Locator] = true
			return s
		})
		return len(blocks)
	}
	tRepackNoop := time.Duration(0)
	nRepackNoop := 0
	tRepackTotal := time.Duration(0)
	nRepackTotal := 0
	filesWritten := make(map[string]bool)
	stats := bytes.NewBuffer(nil)
	fmt.Fprint(stats, "writes\tfiles\tbytes_in_files\tblocks\tbytes_written_backend\tn_repacked\tn_repack_noop\tseconds_repacking\n")
	for writeIndex, write := range writes {
		for i, c := range write.path {
			if c == '/' && !dirsCreated[write.path[:i]] {
				testfs.Mkdir(write.path[:i], 0700)
				dirsCreated[write.path[:i]] = true
			}
		}
		f, err := testfs.OpenFile(write.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0700)
		c.Assert(err, check.IsNil)
		filesWritten[write.path] = true
		data := write.data()
		_, err = f.Write(data)
		c.Assert(err, check.IsNil)
		err = f.Close()
		c.Assert(err, check.IsNil)
		bytesContent += len(data)

		_, err = cfs.MarshalManifest("")
		c.Assert(err, check.IsNil)
		t0 := time.Now()
		n, err := cfs.Repack(context.Background(), RepackOptions{})
		c.Assert(err, check.IsNil)
		tRepack := time.Since(t0)
		tRepackTotal += tRepack
		nRepackTotal++

		if n == 0 {
			tRepackNoop += tRepack
			nRepackNoop++
		} else if bytesWritten()/4 > bytesContent {
			// Rewriting data >4x on average means
			// something is terribly wrong -- give up now
			// instead of going OOM.
			c.Logf("something is terribly wrong -- bytesWritten %d >> bytesContent %d", bytesWritten(), bytesContent)
			c.FailNow()
		}
		fmt.Fprintf(stats, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%.06f\n", writeIndex+1, len(filesWritten), bytesContent, blocksInManifest(), bytesWritten(), nRepackTotal-nRepackNoop, nRepackNoop, tRepackTotal.Seconds())
	}
	c.Check(err, check.IsNil)
	c.Check(blocksInManifest() <= maxBlocks, check.Equals, true, check.Commentf("expect %d <= %d", blocksInManifest(), maxBlocks))

	c.Logf("writes %d files %d bytesContent %d bytesWritten %d bytesRewritten %d blocksInManifest %d", len(writes), len(filesWritten), bytesContent, bytesWritten(), bytesWritten()-bytesContent, blocksInManifest())
	c.Logf("spent %v on %d Repack calls, average %v per call", tRepackTotal, nRepackTotal, tRepackTotal/time.Duration(nRepackTotal))
	c.Logf("spent %v on %d Repack calls that had no effect, average %v per call", tRepackNoop, nRepackNoop, tRepackNoop/time.Duration(nRepackNoop))

	if enableRepackCharts {
		// write stats to tmp/{testname}_stats.tsv
		err = os.Mkdir("tmp", 0777)
		if !os.IsExist(err) {
			c.Check(err, check.IsNil)
		}
		err = os.WriteFile("tmp/"+c.TestName()+"_stats.tsv", stats.Bytes(), 0666)
		c.Check(err, check.IsNil)
	}
}

func (s *CollectionFSSuite) TestSnapshotSplice(c *check.C) {
	filedata1 := "hello snapshot+splice world\n"
	fs, err := (&Collection{}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)
	{
		f, err := fs.OpenFile("file1", os.O_CREATE|os.O_RDWR, 0700)
		c.Assert(err, check.IsNil)
		_, err = f.Write([]byte(filedata1))
		c.Assert(err, check.IsNil)
		err = f.Close()
		c.Assert(err, check.IsNil)
	}

	snap, err := Snapshot(fs, "/")
	c.Assert(err, check.IsNil)
	err = Splice(fs, "dir1", snap)
	c.Assert(err, check.IsNil)
	f, err := fs.Open("dir1/file1")
	c.Assert(err, check.IsNil)
	buf, err := io.ReadAll(f)
	c.Assert(err, check.IsNil)
	c.Check(string(buf), check.Equals, filedata1)
}

func (s *CollectionFSSuite) TestRefreshSignatures(c *check.C) {
	filedata1 := "hello refresh signatures world\n"
	fs, err := (&Collection{}).FileSystem(s.client, s.kc)
	c.Assert(err, check.IsNil)
	fs.Mkdir("d1", 0700)
	f, err := fs.OpenFile("d1/file1", os.O_CREATE|os.O_RDWR, 0700)
	c.Assert(err, check.IsNil)
	_, err = f.Write([]byte(filedata1))
	c.Assert(err, check.IsNil)
	err = f.Close()
	c.Assert(err, check.IsNil)

	filedata2 := "hello refresh signatures universe\n"
	fs.Mkdir("d2", 0700)
	f, err = fs.OpenFile("d2/file2", os.O_CREATE|os.O_RDWR, 0700)
	c.Assert(err, check.IsNil)
	_, err = f.Write([]byte(filedata2))
	c.Assert(err, check.IsNil)
	err = f.Close()
	c.Assert(err, check.IsNil)
	txt, err := fs.MarshalManifest(".")
	c.Assert(err, check.IsNil)
	var saved Collection
	err = s.client.RequestAndDecode(&saved, "POST", "arvados/v1/collections", nil, map[string]interface{}{
		"select": []string{"manifest_text", "uuid", "portable_data_hash"},
		"collection": map[string]interface{}{
			"manifest_text": txt,
		},
	})
	c.Assert(err, check.IsNil)

	// Update signatures synchronously if they are already expired
	// when Read() is called.
	{
		saved.ManifestText = SignManifest(saved.ManifestText, s.kc.authToken, time.Now().Add(-2*time.Second), s.kc.sigttl, []byte(s.kc.sigkey))
		fs, err := saved.FileSystem(s.client, s.kc)
		c.Assert(err, check.IsNil)
		f, err := fs.OpenFile("d1/file1", os.O_RDONLY, 0)
		c.Assert(err, check.IsNil)
		buf, err := ioutil.ReadAll(f)
		c.Check(err, check.IsNil)
		c.Check(string(buf), check.Equals, filedata1)
	}

	// Update signatures asynchronously if we're more than half
	// way to TTL when Read() is called.
	{
		exp := time.Now().Add(2 * time.Minute)
		saved.ManifestText = SignManifest(saved.ManifestText, s.kc.authToken, exp, s.kc.sigttl, []byte(s.kc.sigkey))
		fs, err := saved.FileSystem(s.client, s.kc)
		c.Assert(err, check.IsNil)
		f1, err := fs.OpenFile("d1/file1", os.O_RDONLY, 0)
		c.Assert(err, check.IsNil)
		f2, err := fs.OpenFile("d2/file2", os.O_RDONLY, 0)
		c.Assert(err, check.IsNil)
		buf, err := ioutil.ReadAll(f1)
		c.Check(err, check.IsNil)
		c.Check(string(buf), check.Equals, filedata1)

		// Ensure fs treats the 2-minute TTL as less than half
		// the server's signing TTL. If we don't do this,
		// collectionfs will guess the signature is fresh,
		// i.e., signing TTL is 2 minutes, and won't do an
		// async refresh.
		fs.(*collectionFileSystem).guessSignatureTTL = time.Hour

		refreshed := false
		for deadline := time.Now().Add(time.Second * 10); time.Now().Before(deadline) && !refreshed; time.Sleep(time.Second / 10) {
			_, err = f1.Seek(0, io.SeekStart)
			c.Assert(err, check.IsNil)
			buf, err = ioutil.ReadAll(f1)
			c.Assert(err, check.IsNil)
			c.Assert(string(buf), check.Equals, filedata1)
			loc := s.kc.reads[len(s.kc.reads)-1]
			t, err := signatureExpiryTime(loc)
			c.Assert(err, check.IsNil)
			c.Logf("last read block %s had signature expiry time %v", loc, t)
			if t.Sub(time.Now()) > time.Hour {
				refreshed = true
			}
		}
		c.Check(refreshed, check.Equals, true)

		// Second locator should have been updated at the same
		// time.
		buf, err = ioutil.ReadAll(f2)
		c.Assert(err, check.IsNil)
		c.Assert(string(buf), check.Equals, filedata2)
		loc := s.kc.reads[len(s.kc.reads)-1]
		c.Check(loc, check.Not(check.Equals), s.kc.reads[len(s.kc.reads)-2])
		t, err := signatureExpiryTime(s.kc.reads[len(s.kc.reads)-1])
		c.Assert(err, check.IsNil)
		c.Logf("last read block %s had signature expiry time %v", loc, t)
		c.Check(t.Sub(time.Now()) > time.Hour, check.Equals, true)
	}
}

var bigmanifest = func() string {
	var buf bytes.Buffer
	for i := 0; i < 2000; i++ {
		fmt.Fprintf(&buf, "./dir%d", i)
		for i := 0; i < 100; i++ {
			fmt.Fprintf(&buf, " d41d8cd98f00b204e9800998ecf8427e+99999")
		}
		for i := 0; i < 2000; i++ {
			fmt.Fprintf(&buf, " 1200000:300000:file%d", i)
		}
		fmt.Fprintf(&buf, "\n")
	}
	return buf.String()
}()

func (s *CollectionFSSuite) BenchmarkParseManifest(c *check.C) {
	DebugLocksPanicMode = false
	c.Logf("test manifest is %d bytes", len(bigmanifest))
	for i := 0; i < c.N; i++ {
		fs, err := (&Collection{ManifestText: bigmanifest}).FileSystem(s.client, s.kc)
		c.Check(err, check.IsNil)
		c.Check(fs, check.NotNil)
	}
}

func (s *CollectionFSSuite) checkMemSize(c *check.C, f File) {
	fn := f.(*filehandle).inode.(*filenode)
	var memsize int64
	for _, seg := range fn.segments {
		if e, ok := seg.(*memSegment); ok {
			memsize += int64(len(e.buf))
		}
	}
	c.Check(fn.memsize, check.Equals, memsize)
}

type CollectionFSUnitSuite struct{}

var _ = check.Suite(&CollectionFSUnitSuite{})

// expect ~2 seconds to load a manifest with 256K files
func (s *CollectionFSUnitSuite) TestLargeManifest_ManyFiles(c *check.C) {
	if testing.Short() {
		c.Skip("slow")
	}
	s.testLargeManifest(c, 512, 512, 1, 0)
}

func (s *CollectionFSUnitSuite) TestLargeManifest_LargeFiles(c *check.C) {
	if testing.Short() {
		c.Skip("slow")
	}
	s.testLargeManifest(c, 1, 800, 1000, 0)
}

func (s *CollectionFSUnitSuite) TestLargeManifest_InterleavedFiles(c *check.C) {
	if testing.Short() {
		c.Skip("slow")
	}
	// Timing figures here are from a dev host, (0)->(1)->(2)->(3)
	// (0) no optimizations (main branch commit ea697fb1e8)
	// (1) resolve streampos->blkidx with binary search
	// (2) ...and rewrite PortableDataHash() without regexp
	// (3) ...and use fnodeCache in loadManifest
	s.testLargeManifest(c, 1, 800, 100, 4<<20) // 127s    -> 12s  -> 2.5s -> 1.5s
	s.testLargeManifest(c, 1, 50, 1000, 4<<20) // 44s     -> 10s  -> 1.5s -> 0.8s
	s.testLargeManifest(c, 1, 200, 100, 4<<20) // 13s     -> 4s   -> 0.6s -> 0.3s
	s.testLargeManifest(c, 1, 200, 150, 4<<20) // 26s     -> 4s   -> 1s   -> 0.5s
	s.testLargeManifest(c, 1, 200, 200, 4<<20) // 38s     -> 6s   -> 1.3s -> 0.7s
	s.testLargeManifest(c, 1, 200, 225, 4<<20) // 46s     -> 7s   -> 1.5s -> 1s
	s.testLargeManifest(c, 1, 400, 400, 4<<20) // 477s    -> 24s  -> 5s   -> 3s
	// s.testLargeManifest(c, 1, 800, 1000, 4<<20) // timeout -> 186s -> 28s  -> 17s
}

func (s *CollectionFSUnitSuite) testLargeManifest(c *check.C, dirCount, filesPerDir, blocksPerFile, interleaveChunk int) {
	t0 := time.Now()
	const blksize = 1 << 26
	c.Logf("%s building manifest with dirCount=%d filesPerDir=%d blocksPerFile=%d", time.Now(), dirCount, filesPerDir, blocksPerFile)
	mb := bytes.NewBuffer(make([]byte, 0, 40000000))
	blkid := 0
	for i := 0; i < dirCount; i++ {
		fmt.Fprintf(mb, "./dir%d", i)
		for j := 0; j < filesPerDir; j++ {
			for k := 0; k < blocksPerFile; k++ {
				blkid++
				fmt.Fprintf(mb, " %032x+%d+A%040x@%08x", blkid, blksize, blkid, blkid)
			}
		}
		for j := 0; j < filesPerDir; j++ {
			if interleaveChunk == 0 {
				fmt.Fprintf(mb, " %d:%d:dir%d/file%d", (filesPerDir-j-1)*blocksPerFile*blksize, blocksPerFile*blksize, j, j)
				continue
			}
			for todo := int64(blocksPerFile) * int64(blksize); todo > 0; todo -= int64(interleaveChunk) {
				size := int64(interleaveChunk)
				if size > todo {
					size = todo
				}
				offset := rand.Int63n(int64(blocksPerFile)*int64(blksize)*int64(filesPerDir) - size)
				fmt.Fprintf(mb, " %d:%d:dir%d/file%d", offset, size, j, j)
			}
		}
		mb.Write([]byte{'\n'})
	}
	coll := Collection{ManifestText: mb.String()}
	c.Logf("%s built manifest size=%d", time.Now(), mb.Len())

	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)
	c.Logf("%s Alloc=%d Sys=%d", time.Now(), memstats.Alloc, memstats.Sys)

	f, err := coll.FileSystem(NewClientFromEnv(), &keepClientStub{})
	c.Check(err, check.IsNil)
	c.Logf("%s loaded", time.Now())
	c.Check(f.Size(), check.Equals, int64(dirCount*filesPerDir*blocksPerFile*blksize))

	// Stat() and OpenFile() each file. This mimics the behavior
	// of webdav propfind, which opens each file even when just
	// listing directory entries.
	for i := 0; i < dirCount; i++ {
		for j := 0; j < filesPerDir; j++ {
			fnm := fmt.Sprintf("./dir%d/dir%d/file%d", i, j, j)
			fi, err := f.Stat(fnm)
			c.Assert(err, check.IsNil)
			c.Check(fi.IsDir(), check.Equals, false)
			f, err := f.OpenFile(fnm, os.O_RDONLY, 0)
			c.Assert(err, check.IsNil)
			f.Close()
		}
	}
	c.Logf("%s OpenFile() x %d", time.Now(), dirCount*filesPerDir)

	runtime.ReadMemStats(&memstats)
	c.Logf("%s Alloc=%d Sys=%d", time.Now(), memstats.Alloc, memstats.Sys)
	c.Logf("%s MemorySize=%d", time.Now(), f.MemorySize())
	c.Logf("%s ... test duration %s", time.Now(), time.Now().Sub(t0))
}

// Gocheck boilerplate
func Test(t *testing.T) {
	check.TestingT(t)
}
