ClientBuilder
<<<<<<<<<<<<<

**codec_connection_preparation_latency_ms**
  a histogram of the length of time it takes to prepare a connection and get back a service,
  regardless of success or failure

StatsServiceFactory
<<<<<<<<<<<<<<<<<<<

**available**
  a gauge of whether the underlying factory is available (1) or not (0).  finagle uses this
  primarily to decide whether a host is available to make new connections to in the load balancer.
