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
	"errors"
	"io"
	"net/url"
)

// Objects describing how to watch a specific type of files, identified
// by their URL schema. Watch will be invoked whenever watching a file
// with the given schema is requested.
type WatcherCreator interface {
	Watch(*url.URL, func(string, io.ReadCloser)) (Watcher, error)
}

// Watchers are the objects doing the actual watching of individual
// files. They are configured by the WatcherCreator and will continue
// invoking their configured handlers until Shutdown() is called.
type Watcher interface {
	// Stop listening for notifications on the given file. This may take
	// until the next event to take effect.
	Shutdown() error

	// Retrieve the error channel associated with the watcher.
	// It will stream a list of all errors created while watching.
	ErrChan() chan error
}

// List of URL schema handlers known.
var fileWatcherHandlers map[string]WatcherCreator = make(map[string]WatcherCreator)

// Register "creator" as a handler for watchers for all URLs with the given
// "schema".
func RegisterWatcher(schema string, creator WatcherCreator) {
	fileWatcherHandlers[schema] = creator
}

// Watch the given "fileurl" for changes, sending all of them to the specified
// "handler". This will look up the required handler for the scheme specified
// in the URL and forward the watch request. A Watcher object is returned
// which can be used to stop watching, as defined by the individual watchers.
func Watch(fileurl *url.URL, handler func(string, io.ReadCloser)) (Watcher, error) {
	var creator WatcherCreator
	var ok bool

	creator, ok = fileWatcherHandlers[fileurl.Scheme]
	if !ok {
		return nil, errors.New("No handler registered for \"" +
			fileurl.Scheme + "\"")
	}

	return creator.Watch(fileurl, handler)
}
