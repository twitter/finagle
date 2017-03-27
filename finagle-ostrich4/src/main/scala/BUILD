scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-ostrich4',
    repo = artifactory,
  ),
  dependencies=[
    'ostrich',
    'util/util-registry',
    'util/util-stats',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-http/src/main/scala:scala',
    'finagle/finagle-ostrich4/src/main/resources',
    'util/util-app/src/main/scala:scala',
    'util/util-core/src/main/scala:scala',
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala'),
  # Exposes a service-loaded class.
  scope='forced',
)
