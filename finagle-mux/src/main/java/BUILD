java_library(name='java',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-mux-java',
    repo = artifactory,
  ),
  fatal_warnings=True,
  sources=rglobs('*.java'),
  dependencies=[
    '3rdparty/jvm/org/scala-lang:scala-library',
    'finagle/finagle-mux/src/main/scala'
  ]
)
