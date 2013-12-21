junit_tests(name='scala',
  dependencies=[
    pants('3rdparty:junit'),
    pants('3rdparty:scalatest'),
    jar(org = 'com.twitter.common', name = 'metrics-data-sample', rev = '0.0.1'),
    pants('finagle/finagle-testers')
  ],
  sources=rglobs('*.scala')
)
