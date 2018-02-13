**<server_label>/thriftmux/connects**
  A counter of the number of times the server has created a ThriftMux
  connection. This does not include downgraded Thrift connections.

**<server_label>/thriftmux/downgraded_connects**
  A counter of the number of times the server has created a downgraded
  connection for "plain" Thrift.

**<server_label>/thriftmux/tls/upgrade/success**
  A counter of the number of times the client or server has successfully
  upgraded a connection to TLS.

**<server_label>/mux/tls/upgrade/incompatible**
  A counter of the number of times a client or server failed to establish a session
  due to incompatible TLS requirements or capabilities.
