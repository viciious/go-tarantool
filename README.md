<a href="http://tarantool.org">
	<img src="https://avatars2.githubusercontent.com/u/2344919?v=2&s=250" align="right">
</a>

# Client in Go for Tarantool 1.6+

The `go-tarantool` package has everything necessary for interfacing with
[Tarantool 1.6+](http://tarantool.org/).

The advantage of integrating Go with Tarantool, which is an application server
plus a DBMS, is that Go programmers can handle databases and perform on-the-fly
recompilations of embedded Lua routines, just as in C, with responses that are
faster than other packages according to public benchmarks.

## Table of contents

* [Installation](#installation)
* [Hello World](#hello-world)
* [API reference](#api-reference)
* [Walking\-through example in Go](#walking-through-example-in-go)
* [Help](#help)

## Installation

We assume that you have Tarantool version 1.6 or 1.7, and a modern Linux or BSD
operating system.

You will need a current version of `go`, version 1.3 or later (use
`go version` to check the version number). Do not use `gccgo-go`.

If your `go` version is older than 1.3, or if `go` is not installed,
download the latest tarball from [golang.org](https://golang.org/dl/) and say:

```bash
sudo tar -C /usr/local -xzf go1.8.3.linux-amd64.tar.gz
sudo chmod -R a+rwx /usr/local/go
```

Make sure `go` and `go-tarantool` are on your path. For example:

```
export PATH=$PATH:/usr/local/go/bin
export GOPATH="/usr/local/go/go-tarantool"
```

The `go-tarantool` package is in the 
[viciious/go-tarantool](https://github.com/viciious/go-tarantool) repository.
To download and install, say:

```
go get github.com/viciious/go-tarantool
```

This should bring source and binary files into subdirectories of `/usr/local/go`,
making it possible to access by adding `github.com/viciious/go-tarantool` in
the `import {...}` section at the start of any Go program.

<h2>Hello World</h2>

Here is a very short Go program which tries to connect to a Tarantool server.

```
package main

import (
    "fmt"
    "github.com/viciious/go-tarantool"
)

func main() {
    opts := tarantool.Options{User: "guest"}
    conn, err := tarantool.Connect("127.0.0.1:3301", &opts)
    if err != nil {
        fmt.Println("Connection refused: %s", err.Error())
    }
    conn.Close()
}
```

Cut and paste this example into a file named example.go.

Start a Tarantool server on localhost, and make sure it is listening
on port 3301. Again, make sure GOPATH points to the right place.
Start the program by saying:

Then cut and paste the example into a file named `example.go`,
and run it:

go build example.go

You should see: nothing.

If that is what you see, then you have successfully installed `go-tarantool` and
successfully executed a program that connected to a Tarantool server.

<h2>API reference</h2>

Read the [Tarantool manual](http://tarantool.org/doc.html) to find descriptions
of terms like "connect", "space", "index", and the requests for creating and
manipulating database objects or Lua functions.

The source files for the requests library are:
* [connection.go](https://github.com/viciious/go-tarantool/blob/master/connector.go)
  for the `Connect()` function plus functions related to connecting, and
* [insert_test.go](https://github.com/viciious/go-tarantool/blob/master/insert_test.go)
  for an example of a data-manipulation function used in tests.

See comments in these files for syntax details:
```
call.go
delete.go
eval.go
insert.go
iterator.go
join.go
operator.go
pack.go
update.go
upsert.go
```

The supported requests have parameters and results equivalent to requests in the
Tarantool manual. Browsing through the other *.go programs in the package will
show how the packagers have paid attention to some of the more advanced features
of Tarantool, such as vclock and replication.

## Walking through the example

We can now have a closer look at the `example.go` program and make some observations
about what it does.

```
package main

import (
    "context"
    "fmt"
    "github.com/viciious/go-tarantool"
)

func main() {
    opts := &tarantool.Options{User: "guest"}
    conn, err := tarantool.Connect("127.0.0.1:3301", opts)
    // conn, err := tarantool.Connect("/path/to/tarantool.socket", opts)
    if err != nil {
        fmt.Println("Connection refused: %s", err.Error())
    }

    res := conn.Exec(context.Background(), &tarantool.Insert{Space: "tester", Tuple: []interface{}{uint64(4), "Hello"}})
    if res.Error != nil {
        fmt.Println("Error", res.Error)
    } else {
        fmt.Println(fmt.Sprintf("Insert success: %#v", res.Data))
    }
}
```

**Observation 1:** the line "`github.com/viciious/go-tarantool`" in the
`import(...)` section brings in all Tarantool-related functions and structures.

**Observation 2:** the line beginning with "`Opts :=`" sets up the options for
`Connect()`. In this example, there is only one thing in the structure, a user
name. The structure can also contain:

* `ConnectTimeout` 
* `QueryTimeout`    (the default maximum number of milliseconds to wait before giving up - can be overriden on per-query basis),
* `DefaultSpace`    (the name of default Tarantool space)
* `Password`        (user's password)
* `UUID`            (used for replication)
* `ReplicaSetUUID`  (used for replication)

**Observation 3:** the line containing "`tarantool.Connect`" is essential for
beginning any session. There are two parameters:

* a string with `host:port` format, and
* the option structure that was set up earlier.

**Observation 4:** the `err` structure will be `nil` if there is no error,
otherwise it will have a description which can be retrieved with `err.Error()`.

## Help

To contact `go-tarantool` developers on any problems, create an issue at
[viciious/go-tarantool](http://github.com/viciious/go-tarantool/issues).

The developers of the [Tarantool server](http://github.com/tarantool/tarantool)
will also be happy to provide advice or receive feedback.
