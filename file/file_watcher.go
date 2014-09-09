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

package file

import (
	"io"
	"net/url"
	"os"

	"code.google.com/p/go.exp/fsnotify"
	"github.com/caoimhechaos/go-file"
)

// Object for generating watchers for individual files.
type FileWatcherCreator struct {
}

// Create a new watcher object for watching for notifications on the
// given URL.
func (f *FileWatcherCreator) Watch(
	fileid *url.URL, cb func(string, io.ReadCloser)) (
	file.Watcher, error) {
	return NewFileWatcher(fileid.Path, cb)
}

// Object for watching an individual file for changes.
type FileWatcher struct {
	cb       func(string, io.ReadCloser)
	watcher  *fsnotify.Watcher
	path     string
	shutdown bool
}

// Automatically sign us up for file:// URLs.
func init() {
	file.RegisterWatcher("file", &FileWatcherCreator{})
}

// Create a new FileWatcher watching the file at "path".
func NewFileWatcher(path string, cb func(string, io.ReadCloser)) (
	*FileWatcher, error) {
	var ret *FileWatcher
	var watcher *fsnotify.Watcher
	var err error

	watcher, err = fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	err = watcher.Watch(path)
	if err != nil {
		return nil, err
	}

	ret = &FileWatcher{
		cb:      cb,
		watcher: watcher,
		path:    path,
	}

	go ret.watchForChanges()

	return ret, nil
}

// Read events happening on the file being watched and forward them
// to the relevant callback.
func (f *FileWatcher) watchForChanges() {
	for !f.shutdown {
		var event *fsnotify.FileEvent

		event = <-f.watcher.Event

		if event.IsModify() {
			var fn *os.File
			var err error

			fn, err = os.Open(event.Name)
			if err == nil {
				go f.cb(event.Name, fn)
			} else {
				f.watcher.Error <- err
			}
		}
	}
}

// Stop listening for notifications on the file.
func (f *FileWatcher) Shutdown() error {
	var err error

	f.shutdown = true
	err = f.watcher.RemoveWatch(f.path)
	if err != nil {
		return err
	}

	return f.watcher.Close()
}

// Retrieve the error channel associated with the watcher.
// It will stream a list of all errors created while watching.
func (f *FileWatcher) ErrChan() chan error {
	return f.watcher.Error
}
