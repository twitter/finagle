java_thrift_library(name='thrift-scala',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-benchmark',
    repo = pants('pants-support/ivy:internal'),
  ),
  sources=rglobs('*.thrift'),
  compiler='scrooge',
  language='scala',
  rpc_style='finagle',
)
