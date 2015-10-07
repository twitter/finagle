namespace java com.twitter.finagle.thrift.thriftjava
#@namespace scala com.twitter.finagle.thrift.thriftscala

service Echo {
  string echo(string msg);
}

service ExtendedEcho extends Echo {
  string getStatus();
}
