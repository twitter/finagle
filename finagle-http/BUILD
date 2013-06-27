maven_layout()

jar_library(name='finagle-http',
  dependencies=[
    pants('finagle/finagle-http/src/main/scala')
  ]
)

jar_library(name='tests',
  dependencies=[
    pants('finagle/finagle-http/src/test/java'),
    pants('finagle/finagle-http/src/test/scala')
  ]
)
