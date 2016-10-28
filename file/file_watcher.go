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

	"github.com/caoimhechaos/go-file"
	"gopkg.in/fsnotify.v1"
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

// Resolve an absolute and a relative path to a new absolute path.
func resolveRelative(orig, relative string) (path string, err error) {
	var origurl, newurl *url.URL

	origurl, err = url.Parse(orig)
	if err != nil {
		return
	}

	newurl, err = origurl.Parse(relative)
	if err != nil {
		return
	}

	path = newurl.String()
	return
}

// Automatically sign us up for file:// URLs.
func init() {
	file.RegisterWatcher("file", &FileWatcherCreator{})
}

// Create a new FileWatcher watching the file at "path".
func NewFileWatcher(path string, cb func(string, io.ReadCloser)) (
	*FileWatcher, error) {
	var fi os.FileInfo
	var ret *FileWatcher
	var watcher *fsnotify.Watcher
	var err error

	watcher, err = fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	err = watcher.Add(path)
	if err != nil {
		return nil, err
	}

	ret = &FileWatcher{
		cb:      cb,
		watcher: watcher,
		path:    path,
	}

	// Treat the current state of the file as the first change.
	fi, err = os.Stat(path)
	if err != nil {
		return nil, err
	}

	if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
		var subpath string

		subpath, err = os.Readlink(path)
		if err != nil {
			return nil, err
		}

		path, err = resolveRelative(path, subpath)
		if err != nil {
			return nil, err
		}
	}

	if fi.IsDir() {
		var names []string
		var name string
		var f *os.File

		f, err = os.Open(path)
		if err != nil {
			return nil, err
		}

		names, err = f.Readdirnames(-1)
		if err != nil {
			return nil, err
		}

		for _, name = range names {
			var combined string
			var reader *os.File

			combined, err = resolveRelative(path+"/", name)
			if err != nil {
				return nil, err
			}

			reader, err = os.Open(combined)
			cb(combined, reader)
		}

		f.Close()
	} else {
		var reader *os.File

		reader, err = os.Open(path)
		if err != nil {
			return nil, err
		}

		cb(path, reader)
	}

	go ret.watchForChanges()

	return ret, nil
}

// Read events happening on the file being watched and forward them
// to the relevant callback.
func (f *FileWatcher) watchForChanges() {
	for !f.shutdown {
		var event fsnotify.Event

		event = <-f.watcher.Events

		if event.Op&(fsnotify.Write|fsnotify.Remove|fsnotify.Rename) != 0 {
			var fn *os.File
			var err error

			fn, err = os.Open(event.Name)
			if err == nil {
				go f.cb(event.Name, fn)
			} else {
				f.watcher.Errors <- err
			}
		}
	}
}

// Stop listening for notifications on the file.
func (f *FileWatcher) Shutdown() error {
	var err error

	f.shutdown = true
	err = f.watcher.Remove(f.path)
	if err != nil {
		return err
	}

	return f.watcher.Close()
}

// Retrieve the error channel associated with the watcher.
// It will stream a list of all errors created while watching.
func (f *FileWatcher) ErrChan() chan error {
	return f.watcher.Errors
}
