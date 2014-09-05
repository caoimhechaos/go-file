go-file
=======

go-file essentially tries to provide a wrapper for file-like objects of
different backend types. It uses URLs to identify files (be they local, in
Doozer or somewhere else) and provide relatively transparent access to them.

At this point, it only supports watching files in different types of file
systems, but there is support planned for implementing transparent access to
readers and lateron even writers.

Watchers
--------

So far, watchers are the only implemented common method. They can be used
to watch files for changes and receive a callback with the file name and
an io.ReadCloser object with the modified file contents for ease of access.

The basic important function is file.Watch(). It picks the correct handler
for the URL type it's being passed and invokes it. The handler itself will
do its thing in the background to ensure all modifications of the affected
file are noticed and reported.

The callback itself will only see an io.ReadCloser interface to use when it's
being notified of changes. This means that it doesn't require any logic
relevant to the underlying file system implementation to get to the files
contents.

Since the contents of the modified file may actually be irrelevant,
implementations are required to ensure that just closing the file without
reading it means that no nontrivial cost will be incurred; any expensive
initialization should be deferred until the first call to Read().
