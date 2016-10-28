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
)

// Automatically sign us up for file:// URLs.
func init() {
	file.RegisterFileSystem("file", &FileFileSystemIntegration{})
}

// Go File system integration for the local file system.
type FileFileSystemIntegration struct {
}

// Open the file pointed to by "u" for reading.
func (f *FileFileSystemIntegration) Open(u *url.URL) (io.ReadCloser, error) {
	return os.Open(u.Path)
}

// Open the file pointed to by "u" for writing.
func (f *FileFileSystemIntegration) OpenForWrite(u *url.URL) (
	io.WriteCloser, error) {
	return os.OpenFile(u.Path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
}

// Return a list of all files in the directory given in "u".
func (f *FileFileSystemIntegration) List(u *url.URL) ([]string, error) {
	var dir *os.File
	var err error

	dir, err = os.Open(u.Path)
	if err != nil {
		return []string{}, err
	}

	return dir.Readdirnames(-1)
}

// Create a new watcher object for watching for notifications on the
// given URL.
func (f *FileFileSystemIntegration) Watch(
	fileid *url.URL, cb func(string, io.ReadCloser)) (
	file.Watcher, error) {
	return NewFileWatcher(fileid.Path, cb)
}

// Remove the specified file from the file system.
func (f *FileFileSystemIntegration) Remove(u *url.URL) error {
	return os.Remove(u.Path)
}
