include "fb303.thrift"

namespace java com.twitter.test

exception AnException {}

struct SomeStruct {
  1: i32 arg_one;
  2: string arg_two;
}

service F extends fb303.FacebookService {
  i32 another_method(1: i32 a);
}

service A {
  i32 multiply(1: i32 a, 2: i32 b);
}

service B extends A {
  i32 add(1: i32 a, 2: i32 b) throws (1: AnException ae);
  void add_one(1: i32 a, 2: i32 b) throws (1:AnException ae);
  SomeStruct complex_return(1: string some_string);

  oneway void someway();
}
