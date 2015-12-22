scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-test',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/io/netty',
    'util/util-core'
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala'),
)
