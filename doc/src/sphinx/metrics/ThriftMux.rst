**<server_label>/thriftmux/connects**
  A counter of the number of times the server has created a ThriftMux
  connection. This does not include downgraded Thrift connections.

**<server_label>/thriftmux/downgraded_connects**
  A counter of the number of times the server has created a downgraded
  connection for "plain" Thrift.

**<server_label>/thriftmux/connections**
  A gauge of the number of active ThriftMux connections. This does not
  include downgraded Thrift connections.

**<server_label>/thriftmux/downgraded_connections**
  A gauge of the number of active downgraded Thrift connections. These
  are clients that are speaking Thrift, and not ThriftMux, to a
  ThriftMux server.


