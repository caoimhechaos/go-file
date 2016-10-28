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
	"bytes"
	"io"
	"net/url"

	"github.com/caoimhechaos/go-file"
	etcd "github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
)

// Watcher for an individual etcd key, or prefix.
type EtcdWatcher struct {
	etcdClient *etcd.Client
	path       string
	errchan    chan error
	shutdown   chan bool
	cb         func(string, io.ReadCloser)
}

// etcd file watcher implementation.
type EtcdWatcherCreator struct {
	etcdClient *etcd.Client
}

// Create a new etcd watcher on the client "etcdClient". Listen for changes
// of the key / prefix "path", and deliver notifications to "callback".
// Any errors will be returned on the given error channel "errchan.
//
// The callback function will receive the path of the actually modified key
// as the first parameter and a ReadCloser with the files contents as the
// second. The ReadCloser does not do any significant work until Read() is
// invoked for the first time, so it is safe to ignore it and just use this
// to be notified of file modifications.
func NewEtcdWatcher(etcdClient *etcd.Client, path string,
	cb func(string, io.ReadCloser)) (*EtcdWatcher, error) {
	var ret = &EtcdWatcher{
		etcdClient: etcdClient,
		path:       path,
		errchan:    make(chan error),
		shutdown:   make(chan bool),
	}
	go ret.watchForChanges()
	return ret, nil
}

// Create a new watcher object for watching for notifications on the
// given URL.
func (e *EtcdWatcherCreator) Watch(
	file *url.URL, cb func(string, io.ReadCloser)) (
	file.Watcher, error) {
	return NewEtcdWatcher(e.etcdClient, file.Path, cb)
}

// Watch for changes on the EtcdWatcher and send out callbacks as they occur.
func (w *EtcdWatcher) watchForChanges() {
	var ctx context.Context = context.Background()
	var wc etcd.WatchChan
	var wr etcd.WatchResponse
	var shutdown bool

	wc = w.etcdClient.Watch(ctx, w.path)

	select {
	case wr = <-wc:
		var ev *etcd.Event

		for _, ev = range wr.Events {
			w.cb(string(ev.Kv.Key),
				file.NewReadCloserFake(bytes.NewReader(ev.Kv.Value)))
		}

	case shutdown = <-w.shutdown:
		if shutdown {
			return
		}
	}
}

// Shut down the listener after the next change.
// There's no way to stop it immediately, though.
func (w *EtcdWatcher) Shutdown() error {
	w.shutdown <- true
	return nil
}

// Retrieve the error channel associated with the watcher.
// It will stream a list of all errors created while watching.
func (w *EtcdWatcher) ErrChan() chan error {
	return w.errchan
}
