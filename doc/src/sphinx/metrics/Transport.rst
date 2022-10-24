Netty Transporter
<<<<<<<<<<<<<<<<<

**connect_latency_ms**
  A histogram of the length of time it takes for a socket connection (including SSL/TLS handshake)
  to succeed, in milliseconds.

**failed_connect_latency_ms**
  A histogram of the length of time it takes for a socket connection (including SSL/TLS handshake)
  to fail, in milliseconds.

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

**connection_requests** `verbosity:debug`
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
  A histogram of the duration of the lifetime of a connection, in milliseconds.

**connection_received_bytes** `verbosity:debug`
  A histogram of the number of bytes received over the lifetime of a connection.

**connection_sent_bytes** `verbosity:debug`
  A histogram of the number of bytes sent over the lifetime of a connection.

**received_bytes**
  A counter of the total number of received bytes.

**sent_bytes**
  A counter of the total number of sent bytes.

**tcp_retransmits** `verbosity:debug`
  A counter of the number of TCP retransmits that have occurred.

**tcp_send_window_size** `verbosity:debug`
  A histogram of the TCP send window size (in bytes) per channel.

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

**tls/snooped_connects**
  A counter of the number of TLS connections that were detected via snooping.

IdleChannelHandler
<<<<<<<<<<<<<<<<<<

**disconnects/{READER_IDLE,WRITER_IDLE}**
  A counter of the number of times a connection was disconnected because of a
  given idle state.

SSL/TLS
<<<<<<<

**handshake_latency_ms**
   A histogram of the SSL/TLS handshake latency in milliseconds.

**failed_handshake_latency_ms** `verbosity:debug`
   A histogram of the failed SSL/TLS handshake latency in milliseconds.
