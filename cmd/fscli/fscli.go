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

// Command line utility for accessing files through the file system API.
package main

import (
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/caoimhechaos/go-file"
	fdz "github.com/caoimhechaos/go-file/doozer"
	_ "github.com/caoimhechaos/go-file/file"
	"github.com/ha/doozer"
)

func echoFileOnChange(path string, rc io.ReadCloser) {
	fmt.Println(path, " modified.")
	rc.Close()
}

func echoErrors(errchan chan error) {
	var err error

	for err = range errchan {
		fmt.Printf("Error watching: %s\n", err.Error())
	}
}

func main() {
	var doozer_uri, doozer_buri string
	var args []string
	var cmd string
	var u *url.URL
	var err error

	flag.StringVar(&doozer_uri, "doozer-uri", os.Getenv("DOOZER_URI"),
		"Doozer URI to connect to for Doozer operations")
	flag.StringVar(&doozer_buri, "doozer-boot-uri",
		os.Getenv("DOOZER_BOOT_URI"),
		"Doozer boot URI for finding the Doozer servers in --doozer-uri")
	flag.Parse()

	args = flag.Args()
	if len(args) == 0 {
		fmt.Println("Command required")
		os.Exit(1)
	}

	if len(doozer_uri) > 0 {
		var conn *doozer.Conn
		conn, err = doozer.DialUri(doozer_uri, doozer_buri)
		if err != nil {
			fmt.Printf("Error connecting to %s via doozer: %s\n",
				doozer_uri, err.Error())
			os.Exit(1)
		}

		fdz.RegisterFileType(conn)
	}

	cmd = args[0]
	args = args[1:]

	switch cmd {
	case "ls":
		for _, path := range args {
			var names []string
			var name string

			u, err = url.Parse(path)
			if err != nil {
				fmt.Printf("%s: Error parsing: %s\n", path, err.Error())
				continue
			}

			names, err = file.List(u)
			if err != nil {
				fmt.Printf("%s: error listing: %s\n", u.String(), err.Error())
				continue
			}

			if len(args) > 1 {
				fmt.Printf("\n%s:\n\n", u.String())
			}

			for _, name = range names {
				fmt.Println(name)
			}
		}
	case "cat":
		for _, path := range args {
			var rc io.ReadCloser
			u, err = url.Parse(path)
			if err != nil {
				fmt.Printf("%s: Error parsing: %s\n", path, err.Error())
				continue
			}

			rc, err = file.Open(u)
			if err != nil {
				fmt.Printf("%s: error opening: %s\n", u.String(),
					err.Error())
				continue
			}

			_, err = io.Copy(os.Stdout, rc)
			if err != nil {
				fmt.Printf("%s: error copying: %s\n", u.String(),
					err.Error())
			}

			err = rc.Close()
			if err != nil {
				fmt.Printf("%s: error closing: %s\n", u.String(),
					err.Error())
			}
		}
	case "write":
		var wc io.WriteCloser
		if len(args) != 1 {
			fmt.Print("Wrong number of arguments to write (expected " +
				"file name)")
			os.Exit(1)
		}
		u, err = url.Parse(args[0])
		if err != nil {
			fmt.Printf("%s: Error parsing: %s\n", args[0], err.Error())
			os.Exit(1)
		}

		wc, err = file.OpenForWrite(u)
		if err != nil {
			fmt.Printf("%s: error opening: %s\n", u.String(),
				err.Error())
			os.Exit(1)
		}

		_, err = io.Copy(wc, os.Stdin)
		if err != nil {
			fmt.Printf("%s: error copying: %s\n", u.String(),
				err.Error())
		}

		err = wc.Close()
		if err != nil {
			fmt.Printf("%s: error closing: %s\n", u.String(), err.Error())
		}
	case "watch":
		var devnull *os.File
		var watchers []file.Watcher = make([]file.Watcher, len(args))
		var watcher file.Watcher

		for i, path := range args {

			u, err = url.Parse(path)
			if err != nil {
				fmt.Printf("%s: Error parsing: %s\n", path, err.Error())
				continue
			}

			watcher, err = file.Watch(u, echoFileOnChange)
			if err != nil {
				fmt.Printf("%s: error watching: %s\n", u.String(),
					err.Error())
				continue
			}

			go echoErrors(watcher.ErrChan())
			watchers[i] = watcher
		}

		// Wait for the user to press Ctrl-D.
		devnull, err = os.Open(os.DevNull)
		if err != nil {
			fmt.Printf("Error opening %s: %s", os.DevNull, err.Error())
		}
		io.Copy(devnull, os.Stdin)
		devnull.Close()

		for _, watcher = range watchers {
			watcher.Shutdown()
		}
	default:
		fmt.Printf("Command not implemented: %s\n", cmd)
		os.Exit(1)
	}
}
