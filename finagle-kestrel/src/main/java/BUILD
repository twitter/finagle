java_library(name='java',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-kestrel-java',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/org/scala-lang:scala-library',
    'finagle/finagle-kestrel/src/main/scala',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-memcached/src/main/scala:scala',
    'util/util-core/src/main/scala:scala',
  ],
  fatal_warnings=True,
  sources=rglobs('*.java')
)
