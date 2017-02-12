java_library(
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-http-java',
    repo = artifactory,
  ),
  dependencies=[
    'finagle/finagle-base-http/src/main/scala:scala',
    'finagle/finagle-http/src/main/scala',
  ],
  fatal_warnings=True,
  sources=rglobs('*.java'),
)
