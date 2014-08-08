namespace java com.twitter.finagle.topo.thriftjava
#@namespace scala com.twitter.finagle.topo.thriftscala

service Backend {
  string request(1: i32 responseSize, 2: i32 responseLatencyMs);
}
