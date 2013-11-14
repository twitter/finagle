ClientChannelTransport
<<<<<<<<<<<<<<<<<<<<<<

**concurrent_request**
  a counter of the total number of requests which are written concurrently to the client
  channel transport.  if this stat is incremented, the request is failed, because
  ClientChannelTransport is serial.

**orphan_response**
  a counter of the number of responses which get orphaned. if this stat is incremented, your
  transport has already closed.  you should never see this stat.

ChannelConnector
<<<<<<<<<<<<<<<<

**connect_latency_ms**
  a histogram of the length of time it takes for a connection to succeed, in milliseconds

**failed_connect_latency_ms**
  a histogram of the length of time it takes for a connection to fail, in milliseconds

**cancelled_connects**
  a counter of the number of attempts to connect that were cancelled before they succeeded

ServerBridge
<<<<<<<<<<<<

**read_timeout**
  a counter of the number of times the netty channel has caught a ReadTimeoutException.
  this should be the number of timeouts when reading

**write_timeout**
  a counter of the number of times the netty channel has caught a WriteTimeoutException.
  this should be the number of timeouts when writing

ChannelRequestStatsHandler
<<<<<<<<<<<<<<<<<<<<<<<<<<

**connection_requests**
  a histogram of the number of requests received over the lifetime of a connection

ChannelStatsHandler
<<<<<<<<<<<<<<<<<<<

**connects**
  a counter of the total number of successful connections made

**connection_duration**
  a histogram of the duration of the lifetime of a connection

**connection_received_bytes**
  a histogram of the number of bytes received over the lifetime of a connection

**connection_sent_bytes**
  a histogram of the number of bytes sent over the lifetime of a connection

**received_bytes**
  a counter of the total number of received bytes

**sent_bytes**
  a counter of the total number of sent bytes

**closechans**
  a counter of the total number of connections closed

**writableDuration**
  a gauge of the length of time the socket has been writable in the channel

**unwritableDuration**
  a gauge of the length of time the socket has been unwritable in the channel

**connections**
  a gauge of the total number of connections that are currently open in the channel

IdleChannelHandler
<<<<<<<<<<<<<<<<<<

**disconnects/{READER_IDLE,WRITER_IDLE}**
  a counter of the number of times a connection was disconnected because of a given idle state

IdleConnectionFilter
<<<<<<<<<<<<<<<<<<<<

**refused**
  a counter of the number of connections that have been refused because we have hit the high
  watermark of connections full and no connections are idle.

**idle**
  a gauge of the number of connections that are "idle" at this moment

**closed**
  a counter of the number of connections that have been closed for being idle
