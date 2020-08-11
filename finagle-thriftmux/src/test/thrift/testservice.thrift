namespace java com.twitter.finagle.thriftmux.thriftjava
#@namespace scala com.twitter.finagle.thriftmux.thriftscala

exception InvalidQueryException {
  1: i32 errorCode
}

service TestService {
  string query(1: string x) throws (
    1: InvalidQueryException ex
  )

  string question(1: string y) throws (
    1: InvalidQueryException ex
  )

  string inquiry(1: string z) throws (
      1: InvalidQueryException ex
    )
}

service FanoutTestService {
  list<string> query(1: list<string> x)
}
