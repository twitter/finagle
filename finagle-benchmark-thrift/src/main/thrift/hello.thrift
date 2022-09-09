namespace java com.twitter.finagle.benchmark.thriftjava
#@namespace scala com.twitter.finagle.benchmark.thriftscala

service Hello {
  string echo(1: string body);
}

struct Request {
  1: i32 a
  2: i64 b
  3: bool c
  4: string d
  5: list<string> e
  6: map<i32, string> f
  7: set<i64> g
}

struct Response {
  1: i32 a
  2: i64 b
  3: bool c
  4: string d
  5: list<string> e
  6: map<i32, string> f
  7: set<i64> g
}

service ThriftOneGenServer {
  Response echo(Request r)
}