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

// Doozer implementation as a file system layer for go-file.
package doozer

import (
	"io"
	"net/url"

	"github.com/caoimhechaos/go-file"
	"github.com/ha/doozer"
)

// Register the Doozer watcher with the go-file mechanisms.
func RegisterFileType(conn *doozer.Conn) {
	var watcher_creator = &DoozerWatcherCreator{
		conn: conn,
	}
	var fs = &doozerFileSystem{
		conn: conn,
	}
	file.RegisterWatcher("doozer", watcher_creator)
	file.RegisterWatcher("dz", watcher_creator)
	file.RegisterFileSystem("doozer", fs)
	file.RegisterFileSystem("dz", fs)
}

// Object providing all relevant operations for working with Doozer as a
// file system.
type doozerFileSystem struct {
	conn *doozer.Conn
}

// Open the file given as "u" for reading.
func (d *doozerFileSystem) Open(u *url.URL) (io.ReadCloser, error) {
	return NewDoozerReader(d.conn, u.Path), nil
}

// Open the file given as "u" for writing. Any data written to "u"
// will only actually be written to Doozer when Close() is invoked.
func (d *doozerFileSystem) OpenForWrite(u *url.URL) (io.WriteCloser, error) {
	return NewDoozerWriter(d.conn, u.Path), nil
}

// Get a list of all names under "u", which is supposed to be a directory.
func (d *doozerFileSystem) List(u *url.URL) (ret []string, err error) {
	var rev int64

	rev, err = d.conn.Rev()
	if err != nil {
		return
	}

	ret, err = d.conn.Getdir(u.Path, rev, 0, -1)
	return
}

// Create a new watcher object for watching for notifications on the
// given URL.
func (d *doozerFileSystem) Watch(u *url.URL,
	cb func(string, io.ReadCloser)) (file.Watcher, error) {
	return NewDoozerWatcher(d.conn, u.Path, cb), nil
}
