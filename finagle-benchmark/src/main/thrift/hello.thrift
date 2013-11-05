namespace java com.twitter.finagle.benchmark.thrift

service Hello {
  string echo(1: string body);
}
