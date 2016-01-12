ClientBuilder
<<<<<<<<<<<<<

**codec_connection_preparation_latency_ms**
  A histogram of the length of time it takes to prepare a connection and get
  back a service, regardless of success or failure.

StatsServiceFactory
<<<<<<<<<<<<<<<<<<<

**available**
  A gauge of whether the underlying factory is available (1) or not (0).
  Finagle uses this primarily to decide whether a host is eligible for new
  connections in the load balancer.
