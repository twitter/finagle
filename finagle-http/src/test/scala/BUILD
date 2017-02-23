junit_tests(
  dependencies=[
    '3rdparty/jvm/com/google/code/findbugs:jsr305',
    '3rdparty/jvm/com/google/guava',
    scoped('3rdparty/jvm/commons-lang', scope='forced'),
    '3rdparty/jvm/io/netty',
    '3rdparty/jvm/junit',
    '3rdparty/jvm/org/mockito:mockito-all',
    '3rdparty/jvm/org/scalacheck',
    '3rdparty/jvm/org/scalatest',
    'finagle/finagle-base-http',
    'finagle/finagle-core',
    'finagle/finagle-http',
    'finagle/finagle-netty4-http',
    'finagle/finagle-toggle',
    scoped('util/util-app', scope='forced'),
    'util/util-collection',
    'util/util-logging',
    'util/util-stats',
  ],
  resources=['finagle/finagle-http/src/test/resources'],
  fatal_warnings=True,
  sources=rglobs('*.scala')
)
