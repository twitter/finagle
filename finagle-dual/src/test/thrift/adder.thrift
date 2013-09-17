namespace java com.twitter.finagle.dual.thrift

service Adder {
  i32 add(1: i32 x, 2: i32 y)
}
