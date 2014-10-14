
namespace java com.twitter.finagle.demo.thriftjava
#@namespace scala com.twitter.finagle.demo.thriftscala

service Tracing1 {
  string computeSomething();
}

service Tracing2 {
  string computeSomethingElse();
}

service Tracing3 {
  string oneMoreThingToCompute();
}
