namespace java com.twitter.finagle.thrift.thriftjava
#@namespace scala com.twitter.finagle.thrift.thriftscala

exception InvalidQueryException {
  1: i32 errorCode
}

service Echo {
  string echo(string msg) throws (
    1: InvalidQueryException ex
  );
}

service ExtendedEcho extends Echo {
  string getStatus();
}
