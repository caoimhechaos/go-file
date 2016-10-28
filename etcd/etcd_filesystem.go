/*
 * (c) 2016, Caoimhe Chaos <caoimhechaos@protonmail.com>,
 *	     Starship Factory. All rights reserved.
 *
 * Redistribution and use in source  and binary forms, with or without
 * modification, are permitted  provided that the following conditions
 * are met:
 *
 * * Redistributions of  source code  must retain the  above copyright
 *   notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 *   notice, this  list of conditions and the  following disclaimer in
 *   the  documentation  and/or  other  materials  provided  with  the
 *   distribution.
 * * Neither  the name  of the Starship Factory  nor the  name  of its
 *   contributors may  be used to endorse or  promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS"  AND ANY EXPRESS  OR IMPLIED WARRANTIES  OF MERCHANTABILITY
 * AND FITNESS  FOR A PARTICULAR  PURPOSE ARE DISCLAIMED. IN  NO EVENT
 * SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL,  EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED  TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE,  DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT  LIABILITY,  OR  TORT  (INCLUDING NEGLIGENCE  OR  OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package etcd

import (
	"io"
	"net/url"
	"strings"

	"github.com/caoimhechaos/go-file"
	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"golang.org/x/net/context"
)

// etcd file system implementation.
type etcdFileSystem struct {
	etcdClient *etcd.Client
}

// Register the etcd watcher with the go-file mechanisms.
func RegisterEtcdClient(etcdClient *etcd.Client) {
	var watcherCreator = &EtcdWatcherCreator{
		etcdClient: etcdClient,
	}
	var fs = &etcdFileSystem{
		etcdClient: etcdClient,
	}
	file.RegisterWatcher("etcd", watcherCreator)
	file.RegisterFileSystem("etcd", fs)
}

// Open the file given as "u" for reading.
func (e *etcdFileSystem) Open(u *url.URL) (io.ReadCloser, error) {
	return NewEtcdReader(e.etcdClient, u.Path)
}

// Open the file given as "u" for writing. Any data written to "u"
// will only actually be written to Doozer when Close() is invoked.
func (e *etcdFileSystem) OpenForWrite(u *url.URL) (io.WriteCloser, error) {
	return NewEtcdWriter(e.etcdClient, u.Path), nil
}

// Get a list of all names under "u", which is supposed to be a directory.
func (e *etcdFileSystem) List(u *url.URL) (ret []string, err error) {
	var resp *etcd.GetResponse
	var ctx context.Context = context.Background()
	var prefix string = u.Path
	var kv *mvccpb.KeyValue

	// Make sure the prefix is slash delimited.
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	// Now, get all keys which start with the slash-terminated prefix.
	resp, err = e.etcdClient.Get(
		ctx, u.Path, etcd.WithPrefix(), etcd.WithKeysOnly())
	if err != nil {
		return
	}

	for _, kv = range resp.Kvs {
		ret = append(ret, string(kv.Key))
	}
	return
}

// Create a new watcher object for watching for notifications on the
// given URL.
func (e *etcdFileSystem) Watch(u *url.URL,
	cb func(string, io.ReadCloser)) (file.Watcher, error) {
	return NewEtcdWatcher(e.etcdClient, u.Path, cb)
}

// Remove deletes the specified object from the etcd tree.
func (e *etcdFileSystem) Remove(u *url.URL) error {
	var ctx context.Context = context.Background()
	var err error

	_, err = e.etcdClient.Delete(ctx, u.Path)
	return err
}
