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
	"os"

	"github.com/ha/doozer"
)

const (
	MAX_FILE_LEN = (1 << 10)
)

// Doozer writer object. Unlike most other writers, all file contents are
// only written when the writer is closed.
type DoozerWriter struct {
	conn *doozer.Conn
	path string
	buf  *bytes.Buffer
}

// Create a new Doozer writer for the file given at "path", on the connection
// "conn". Any contents in this writer will be written on Close().
func NewDoozerWriter(conn *doozer.Conn, path string) *DoozerWriter {
	return &DoozerWriter{
		conn: conn,
		path: path,
		buf:  new(bytes.Buffer),
	}
}

// Write the bytes given in "b" to the file on Doozer. If the total size
// of the file exceeds 1MB, an "invalid" error (os.ErrInvalid) will be
// returned.
func (d *DoozerWriter) Write(b []byte) (n int, err error) {
	n, err = d.buf.Write(b)
	if err == nil && d.buf.Len() > MAX_FILE_LEN {
		return n, os.ErrInvalid
	}

	return
}

// Write the contents collected so far to the file in Doozer.
func (d *DoozerWriter) Close() error {
	var rev int64
	var err error

	_, rev, err = d.conn.Stat(d.path, nil)
	if err != nil {
		rev, err = d.conn.Rev()
		if err != nil {
			return err
		}
	}

	_, err = d.conn.Set(d.path, rev, d.buf.Bytes())
	return err
}
