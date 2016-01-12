java_library(name='java',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-kestrel-java',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/io/netty',
    '3rdparty/jvm/org/scala-lang:scala-library',
    'finagle/finagle-core',
    'finagle/finagle-memcached',
    'finagle/finagle-kestrel/src/main/scala',
    'util/util-core',
  ],
  fatal_warnings=False,
  sources=rglobs('*.java')
)
