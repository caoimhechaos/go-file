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

package etcd

import (
	"io"
	"sync"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"golang.org/x/net/context"
)

// Reader to read a file from etcd.
//
// Upon the first call to Read(), the entire contents of the file are
// returned. Subsequent calls will return an EOF error.
type EtcdReader struct {
	etcdClient *etcd.Client
	path       string
	wasReadMtx sync.Mutex
	wasRead    bool
}

// Create a new EtcdReader to read the file "path" from the client "etcdClient".
func NewEtcdReader(etcdClient *etcd.Client, path string) (*EtcdReader, error) {
	return &EtcdReader{
		etcdClient: etcdClient,
		path:       path,
	}, nil
}

// Read the contents of the wrapped file and etcd and return them.
func (rd *EtcdReader) Read(p []byte) (int, error) {
	var resp *etcd.GetResponse
	var kv *mvccpb.KeyValue
	var ctx context.Context
	var err error

	rd.wasReadMtx.Lock()
	defer rd.wasReadMtx.Unlock()

	if rd.wasRead {
		return 0, io.EOF
	}
	rd.wasRead = true

	ctx = context.Background()
	resp, err = rd.etcdClient.Get(ctx, rd.path)
	if err != nil {
		return 0, err
	}

	for _, kv = range resp.Kvs {
		p = kv.Value
		return len(p), nil
	}

	return 0, io.EOF
}

// Define the file handle as closed, just in case.
func (rd *EtcdReader) Close() error {
	rd.wasRead = true
	return nil
}
