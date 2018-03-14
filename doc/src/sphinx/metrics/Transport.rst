Netty Transporter
<<<<<<<<<<<<<<<<<

**connect_latency_ms**
  A histogram of the length of time it takes for a connection to succeed,
  in milliseconds.

**failed_connect_latency_ms**
  A histogram of the length of time it takes for a connection to fail,
  in milliseconds.

**cancelled_connects**
  A counter of the number of attempts to connect that were cancelled before
  they succeeded.

ServerBridge
<<<<<<<<<<<<

**read_timeout**
  A counter of the number of times the netty channel has caught a
  ``ReadTimeoutException`` while reading.

**write_timeout**
  A counter of the number of times the netty channel has caught a
  ``WriteTimeoutException`` while writing.

ChannelRequestStatsHandler
<<<<<<<<<<<<<<<<<<<<<<<<<<

**connection_requests**
  A histogram of the number of requests received over the lifetime of a
  connection.

ChannelStatsHandler
<<<<<<<<<<<<<<<<<<<

**connects**
  A counter of the total number of successful connections made.

**closes**
  A counter of the total number of channel close operations initiated. To see the
  total number of closes completed, use the total count from one of the
  "connection_duration", "connection_received_bytes", or "connection_sent_bytes"
  histograms.

**connection_duration** `verbosity:debug`
  A histogram of the duration of the lifetime of a connection.

**connection_received_bytes** `verbosity:debug`
  A histogram of the number of bytes received over the lifetime of a connection.

**connection_sent_bytes** `verbosity:debug`
  A histogram of the number of bytes sent over the lifetime of a connection.

**received_bytes**
  A counter of the total number of received bytes.

**sent_bytes**
  A counter of the total number of sent bytes.

**writableDuration** `verbosity:debug`
  A gauge of the length of time the socket has been writable in the channel.

**unwritableDuration** `verbosity:debug`
  A gauge of the length of time the socket has been unwritable in the channel.

**connections**
  A gauge of the total number of connections that are currently open in the
  channel.

**exn/<exception_name>+**
  A counter of the number of times a specific exception has been thrown within
  a Netty pipeline.

**tls/connections**
  A gauge of the total number of SSL/TLS connections that are currently open in
  the channel.

IdleChannelHandler
<<<<<<<<<<<<<<<<<<

**disconnects/{READER_IDLE,WRITER_IDLE}**
  A counter of the number of times a connection was disconnected because of a
  given idle state.

Thrift
<<<<<<

**srv/thrift/buffer/resetCount**
  A counter for the number of times the thrift server re-initialized the buffer
  for thrift responses. The thrift server maintains a growable reusable buffer
  for responses. Once the buffer reaches the threshold size it is discarded and
  reset to a smaller size. This is done to accommodate variable response sizes.
  A high resetCount means the server is allocating and releasing memory
  frequently. Use the ``com.twitter.finagle.Thrift.param.MaxReusableBufferSize``
  param to set the max buffer size to the size of a typical thrift response for
  your server.
