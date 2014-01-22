namespace java com.twitter.finagle.thriftmux.thriftjava
#@namespace scala com.twitter.finagle.thriftmux.thriftscala

service TestService {
  string query(1: string x)
}
