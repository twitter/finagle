*finagle-mysql* is a MySQL driver built for Finagle. The project provides a simple query API with
support for prepared statements and transactions while leveraging Finagle's
[client stack](http://twitter.github.io/finagle/guide/Clients.html) for connection pooling,
load balancing, etc. See the [API](http://twitter.github.io/finagle/docs/#com.twitter.finagle.exp.Mysql$)
or [docs](http://twitter.github.io/finagle/guide/Protocols.html#mysql) for usage examples.