scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-mdns',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/org/jmdns',
    'finagle/finagle-core/src/main/scala:scala',
    'util/util-core/src/main/scala:scala',
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-mdns/src/main/resources'
  ],
  # Exposes a service-loaded class.
  scope='forced',
)
