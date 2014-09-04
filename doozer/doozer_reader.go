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
	"io"
	"sync"

	"github.com/ha/doozer"
)

// Reader to read a file from Doozer.
//
// Upon the first call to Read(), the entire contents of the file are
// returned. Subsequent calls will return an EOF error.
type DoozerReader struct {
	doozer_conn *doozer.Conn
	path        string
	wasReadMtx  sync.Mutex
	wasRead     bool
}

func NewDoozerReader(conn *doozer.Conn, path string) *DoozerReader {
	return &DoozerReader{
		doozer_conn: conn,
		path:        path,
	}
}

// Read the contents of the specified file on Doozer and return them.
func (dr *DoozerReader) Read(p []byte) (int, error) {
	var l int64
	var err error
	dr.wasReadMtx.Lock()
	defer dr.wasReadMtx.Unlock()

	if dr.wasRead {
		return 0, io.EOF
	}
	dr.wasRead = true
	p, l, err = dr.doozer_conn.Get(dr.path, nil)
	if err != nil {
		return 0, err
	}
	return int(l), nil
}

// Define the file handle as closed, just in case.
func (dr *DoozerReader) Close() error {
	dr.wasRead = true
	return nil
}
