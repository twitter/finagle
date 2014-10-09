java_thrift_library(name='thrift-scala',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-stress-thrift',
    repo = artifactory,
  ),
  sources=rglobs('*.thrift'),
  compiler='scrooge',
  language='scala',
  rpc_style='finagle',
)
