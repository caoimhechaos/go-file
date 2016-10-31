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

// Virtual file system client implementation for Go.
package file

import (
	"errors"
	"io"
	"net/url"
)

var FS_OperationNotImplementedError error = errors.New("Operation not implemented for this file system")

// Object providing all relevant operations for file systems. The individual
// file system backend implementations need to handle these properly, or
// return a FS_OperationNotImplementedError.
type FileSystem interface {
	Open(*url.URL) (io.ReadCloser, error)
	OpenForWrite(*url.URL) (io.WriteCloser, error)
	OpenForAppend(*url.URL) (io.WriteCloser, error)
	List(*url.URL) ([]string, error)
	Watch(*url.URL, func(string, io.ReadCloser)) (Watcher, error)
	Remove(*url.URL) error
}

// List of URL schema handlers known.
var fileSystemHandlers map[string]FileSystem = make(map[string]FileSystem)

// Register "fs" as a file system implementation for all URLs with the given
// "schema".
func RegisterFileSystem(schema string, fs FileSystem) {
	fileSystemHandlers[schema] = fs
}

// Watch the given "fileurl" for changes, sending all of them to the specified
// "handler". This will look up the required handler for the scheme specified
// in the URL and forward the watch request. A Watcher object is returned
// which can be used to stop watching, as defined by the individual watchers.
func Watch(fileurl *url.URL, handler func(string, io.ReadCloser)) (Watcher, error) {
	var creator WatcherCreator
	var fs FileSystem
	var ok bool

	// Prefer the full-filesystem implementation if there is one.
	fs, ok = fileSystemHandlers[fileurl.Scheme]
	if ok {
		return fs.Watch(fileurl, handler)
	}

	// Otherwise, try to find a simple watcher implementation.
	creator, ok = fileWatcherHandlers[fileurl.Scheme]
	if ok {
		return creator.Watch(fileurl, handler)
	}

	return nil, FS_OperationNotImplementedError
}

// Read all names under the given path as file names. Requires "u" to point
// to a directory. The list of file names returned should only be short,
// local names which can be appended to the URL to form a new one.
func List(u *url.URL) ([]string, error) {
	var fs FileSystem
	var ok bool

	fs, ok = fileSystemHandlers[u.Scheme]
	if ok {
		return fs.List(u)
	}

	return nil, FS_OperationNotImplementedError
}

// Return a reader for the file given as "u".
func Open(u *url.URL) (io.ReadCloser, error) {
	var fs FileSystem
	var ok bool

	fs, ok = fileSystemHandlers[u.Scheme]
	if ok {
		return fs.Open(u)
	}

	return nil, FS_OperationNotImplementedError
}

// Return a writer for the file given as "u". Any writer should
// guarantee that all data has been written by the time Close()
// returns without an error. No other guarantees have to be given.
func OpenForWrite(u *url.URL) (io.WriteCloser, error) {
	var fs FileSystem
	var ok bool

	fs, ok = fileSystemHandlers[u.Scheme]
	if ok {
		return fs.OpenForWrite(u)
	}

	return nil, FS_OperationNotImplementedError
}

// Return a writer for appending data to the file given as "u". Any
// writer should guarantee that all data has been written by the time
// Close() returns without an error. No other guarantees have to be
// given.
func OpenForAppend(u *url.URL) (io.WriteCloser, error) {
	var fs FileSystem
	var ok bool

	fs, ok = fileSystemHandlers[u.Scheme]
	if ok {
		return fs.OpenForAppend(u)
	}

	return nil, FS_OperationNotImplementedError
}

// Remove the referenced object from the file system. This would cause
// the file to be deleted from the underlying file system, or whatever
// operation is equivalent to that.
func Remove(u *url.URL) error {
	var fs FileSystem
	var ok bool

	fs, ok = fileSystemHandlers[u.Scheme]
	if ok {
		return fs.Remove(u)
	}

	return FS_OperationNotImplementedError
}
