namespace java com.twitter.finagle.benchmark.thriftjava
#@namespace scala com.twitter.finagle.benchmark.thriftscala

service Hello {
  string echo(1: string body);
}
