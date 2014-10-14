/* 
		CHANGING THIS FILE REQUIRES MANUAL REGENERATION 
		
E.g.:

thrift --gen java test.thrift
cd gen-java
find . -type f -print0 | cpio -pmud0 ../src/test/java

*/

include "fb303.thrift"

namespace java com.twitter.test
#@namespace scala com.twitter.test.thriftscala

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
  
  string show_me_your_dtab();
  i32 show_me_your_dtab_size();
}
