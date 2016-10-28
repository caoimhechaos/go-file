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
	"io"
	"net/url"
	"sync"

	"github.com/caoimhechaos/go-file"
	"github.com/mrkvm/rados.go"
)

// List of all currently open contexts to avoid creating them every time a file
// is accessed.
var openContexts map[string]*rados.Context
var openContextsMtx sync.Mutex

// radosFileSystem implements most of the important file systems on a rados
// backend.
type radosFileSystem struct {
	rfs *rados.Rados
}

// Automatically sign us up for rados:// URLs.
func init() {
	var rfs *rados.Rados
	var err error

	openContexts = make(map[string]*rados.Context)

	rfs, err = rados.NewDefault()
	if err == nil {
		file.RegisterFileSystem("rados", &radosFileSystem{rfs: rfs})
	}
}

// getContext returns the rados context for the specified pool, creating it if
// necessary.
func getContext(r *rados.Rados, pool string) (*rados.Context, error) {
	var ret *rados.Context
	var ok bool
	var err error

	// TODO: this could use read/write locking to be a little more efficient.
	openContextsMtx.Lock()
	defer openContextsMtx.Unlock()

	ret, ok = openContexts[pool]
	if ok && ret != nil {
		return ret, nil
	}

	ret, err = r.NewContext(pool)
	if err != nil {
		return nil, err
	}

	openContexts[pool] = ret
	return ret, err
}

// Register Rados client with a specific configuration file as context.
func RegisterRadosConfig(configPath string) error {
	var rfs *rados.Rados
	var err error

	rfs, err = rados.New(configPath)
	if err != nil {
		return err
	}

	file.RegisterFileSystem("rados", &radosFileSystem{rfs: rfs})
	return nil
}

// Open creates a ReadCloser for the given Rados object. The host name should be
// the name of the Rados pool to fetch objects from.
func (r *radosFileSystem) Open(u *url.URL) (io.ReadCloser, error) {
	var ctx *rados.Context
	var obj *rados.Object
	var err error

	ctx, err = getContext(r.rfs, u.Host)
	if err != nil {
		return nil, err
	}

	obj, err = ctx.Open(u.Path)
	if err != nil {
		return nil, err
	}

	return NewRadosReadCloser(obj), nil
}

// OpenForWrite creates a new WriteCloser for the given Rados object. The writer
// will truncate and append to a given Rados object.
func (r *radosFileSystem) OpenForWrite(u *url.URL) (io.WriteCloser, error) {
	var ctx *rados.Context
	var obj *rados.Object
	var err error

	ctx, err = getContext(r.rfs, u.Host)
	if err != nil {
		return nil, err
	}

	obj, err = ctx.Open(u.Path)
	if err != nil {
		return nil, err
	}

	err = obj.Truncate(0)
	if err != nil {
		return nil, err
	}

	return NewRadosWriteCloser(obj), nil
}

// There is no List function for Rados.
func (*radosFileSystem) List(*url.URL) (r []string, err error) {
	err = file.FS_OperationNotImplementedError
	return
}

// There is no reasonable way to watch a file in Rados.
func (*radosFileSystem) Watch(*url.URL, func(string, io.ReadCloser)) (
	file.Watcher, error) {
	return nil, file.FS_OperationNotImplementedError
}

// Remove the file from Rados. This will remove the named object from all
// ceph object storage replicas.
func (r *radosFileSystem) Remove(u *url.URL) error {
	var ctx *rados.Context
	var err error

	ctx, err = getContext(r.rfs, u.Host)
	if err != nil {
		return err
	}

	return ctx.Remove(u.Path)
}
