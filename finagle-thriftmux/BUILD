maven_layout()

jar_library(name='finagle-thriftmux',
  dependencies=[
    pants('finagle/finagle-thriftmux/src/main/scala')
  ]
)

jar_library(name='tests',
  dependencies=[
    pants('finagle/finagle-thriftmux/src/test/java'),
    pants('finagle/finagle-thriftmux/src/test/scala')
  ]
)
