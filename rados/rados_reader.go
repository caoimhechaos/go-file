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

package rados

import (
	"os"

	"github.com/mrkvm/rados.go"
)

// RadosReadCloser is a simple ReadCloser for Rados files. It will track its
// current position in the Rados file and start reading from it.
type RadosReadCloser struct {
	obj *rados.Object
	pos int64
}

// NewRadosReadCloser creates a new RadosReadCloser for the given Rados object
// "obj".
func NewRadosReadCloser(obj *rados.Object) *RadosReadCloser {
	return &RadosReadCloser{
		obj: obj,
		pos: 0,
	}
}

// Read fetches the next few bytes from the wrapped Rados object and puts them
// into the buffer "p". Up to len(p) bytes will be read at a time.
func (r *RadosReadCloser) Read(p []byte) (n int, err error) {
	n, err = r.obj.ReadAt(p, r.pos)
	if n > 0 {
		r.pos += int64(n)
	}
	return
}

// Seek changes the current position in the file as specified in the parameters.
// See the io.Seeker interface.
func (r *RadosReadCloser) Seek(offset int64, whence int) (int64, error) {
	var newpos int64

	if whence == 0 {
		// Seeking relative to the beginning of the file.
		newpos = offset
	} else if whence == 1 {
		// Seeking relative to the current offset.
		newpos = r.pos + offset
	} else if whence == 2 {
		// Seeking relative to the end of the file.
		newpos = r.obj.Size() + offset
	}

	if newpos < 0 || newpos > r.obj.Size() {
		return -1, os.ErrInvalid
	}

	r.pos = newpos
	return newpos, nil
}

// Close doesn't do a lot since Rados doesn't have that notion.
func (r *RadosReadCloser) Close() error {
	return nil
}
