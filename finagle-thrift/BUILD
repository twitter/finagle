maven_layout()

jar_library(name='finagle-thrift',
  dependencies=[
    pants('finagle/finagle-thrift/src/main/java'),
    pants('finagle/finagle-thrift/src/main/scala')
  ]
)

jar_library(name='tests',
  dependencies=[
    pants('finagle/finagle-thrift/src/test/java'),
    pants('finagle/finagle-thrift/src/test/scala')
  ]
)
