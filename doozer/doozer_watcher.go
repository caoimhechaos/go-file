/*
 * (c) 2014, Caoimhe Chaos <caoimhechaos@protonmail.com>,
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

package doozer

import (
	"bytes"
	"io"
	"net/url"

	"github.com/caoimhechaos/go-file"
	"github.com/ha/doozer"
)

// Watcher for an individual Doozer file (or a subtree).
type DoozerWatcher struct {
	doozer_conn *doozer.Conn
	path        string
	errchan     chan error
	shutdown    bool
	cb          func(string, io.ReadCloser)
}

// File handler integration for the DoozerWatcher class.
type DoozerWatcherCreator struct {
	conn *doozer.Conn
}

// Create a new watcher object for watching for notifications on the
// given URL.
func (d *DoozerWatcherCreator) Watch(
	file *url.URL, cb func(string, io.ReadCloser)) (
	file.Watcher, error) {
	var watcher *DoozerWatcher = NewDoozerWatcher(d.conn, file.Path, cb)
	return watcher, nil
}

// Create a new Doozer watcher on the connection "conn". Listen for changes
// of the file / subtree "path", and deliver notifications to "callback".
// Any errors will be returned on the given error channel "errchan.
//
// The callback function will receive the path of the actually modified file
// as the first parameter and a ReadCloser with the files contents as the
// second. The ReadCloser does not do any significant work until Read() is
// invoked for the first time, so it is safe to ignore it and just use this
// to be notified of file modifications.
func NewDoozerWatcher(conn *doozer.Conn, path string,
	cb func(string, io.ReadCloser)) *DoozerWatcher {
	var ret = &DoozerWatcher{
		doozer_conn: conn,
		path:        path,
		errchan:     make(chan error),
	}
	go ret.watchForChanges()
	return ret
}

// Watch for changes on the specified pattern.
func (dw *DoozerWatcher) watchForChanges() {
	var rev int64
	var err error

	for !dw.shutdown {
		var ev doozer.Event
		var buf *bytes.Reader

		ev, err = dw.doozer_conn.Wait(dw.path, rev)
		if err != nil {
			dw.errchan <- err
			continue
		}

		// If the operation is not SET, it probably won't contain
		// any new data to set.
		if !ev.IsSet() {
			continue
		}

		buf = bytes.NewReader(ev.Body)
		go dw.cb(ev.Path, file.NewReadCloserFake(buf))

		rev = ev.Rev
	}
}

// Shut down the listener after the next change.
// There's no way to stop it immediately, though.
func (dw *DoozerWatcher) Shutdown() error {
	dw.shutdown = true
	return nil
}

// Retrieve the error channel associated with the watcher.
// It will stream a list of all errors created while watching.
func (dw *DoozerWatcher) ErrChan() chan error {
	return dw.errchan
}
