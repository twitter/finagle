java_library(name='java',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-kestrelx-java',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty:netty',
    '3rdparty:scala-library',
    'finagle/finagle-core',
    'finagle/finagle-memcachedx',
    'finagle/finagle-kestrelx/src/main/scala',
    'util/util-core',
  ],
  sources=rglobs('*.java')
)
