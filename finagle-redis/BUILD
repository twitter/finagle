maven_layout()

jar_library(name='finagle-redis',
  dependencies=[
    pants('finagle/finagle-redis/src/main/scala')
  ]
)

jar_library(name='tests',
  dependencies=[
    pants('finagle/finagle-redis/src/test/java'),
    pants('finagle/finagle-redis/src/test/scala')
  ]
)
