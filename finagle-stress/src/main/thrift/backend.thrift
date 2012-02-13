namespace java com.twitter.finagle.topo.thrift

service Backend {
  string request(1: i32 responseSize, 2: i32 responseLatencyMs);
}
