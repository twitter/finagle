java_library(name='java',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-kestrel-java',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty:netty',
    '3rdparty:scala-library',
    'finagle/finagle-core',
    'finagle/finagle-memcached',
    'finagle/finagle-kestrel/src/main/scala',
    'util/util-core',
  ],
  sources=rglobs('*.java')
)
