maven_layout()

jar_library(name='benchmark',
  dependencies=[
    pants('finagle/finagle-exp/src/main/scala:benchmark')
  ]
)

jar_library(name='finagle-exp',
  dependencies=[
    pants('finagle/finagle-exp/src/main/scala')
  ]
)

jar_library(name='tests',
  dependencies=[
    pants('finagle/finagle-exp/src/test/scala')
  ]
)
