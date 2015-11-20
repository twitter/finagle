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

**closes**
  a counter of the total number of channel close operations

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

**exn/<exception_name>+**
  a counter of the number of times a specific exception has been thrown within a Netty pipeline

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

Thrift
<<<<<<

**srv/thrift/buffer/resetCount**
  a counter for the number of times the thrift server re-initialized the buffer for thrift responses. The thrift server maintains a growable reusable buffer for responses. Once the buffer reaches the threshold size it is discarded and reset to a smaller size. This is done to accommodate variable response sizes. A high resetCount means the server is allocating and releasing memory frequently. Use the `com.twitter.finagle.Thrift.param.MaxReusableBufferSize` to set the max buffer size to the size of  a typical thrift response for your server.
