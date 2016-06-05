java_thrift_library(name='thrift-scala',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-benchmark',
    repo = artifactory,
  ),
  sources=rglobs('*.thrift'),
  compiler='scrooge',
  language='scala',
  rpc_style='finagle',
)
