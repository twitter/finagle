namespace java com.twitter.finagle.thriftmux.thrift

service TestService {
  string query(1: string x)
}
