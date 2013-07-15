maven_layout()

jar_library(name='finagle-memcached',
  dependencies=[
    pants('finagle/finagle-memcached/src/main/java'),
    pants('finagle/finagle-memcached/src/main/scala')
  ]
)

jar_library(name='tests',
  dependencies=[
    pants('finagle/finagle-memcached/src/test/java'),
    pants('finagle/finagle-memcached/src/test/scala')
  ]
)
