java_thrift_library(name='thrift-scala',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-example-thrift-scala',
    repo = artifactory,
  ),
  sources=rglobs('*.thrift'),
  compiler='scrooge',
  language='scala',
  rpc_style='finagle',
)

java_thrift_library(name='thrift-java',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-example-thrift-java',
    repo = artifactory,
  ),
  sources=rglobs('*.thrift'),
  compiler='scrooge',
  language='java',
)
