thrift_library(name='thrift',
  sources=rglobs('*.thrift')
)

java_thrift_library(name='thrift-scala',
  sources=rglobs('*.thrift'),
  compiler='scrooge',
  language='scala',
  rpc_style='finagle',
)
