java_library(name='java',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-http-java',
    repo = artifactory,
  ),
  dependencies=[
    'finagle/finagle-http/src/main/scala'
  ],
  fatal_warnings=False,
  sources=rglobs('*.java'),
)
