thrift_library(name='thrift',
  sources=rglobs('*.thrift')
)

compiled_idl(name='scrooge3',
  idl_deps=pants(':thrift'),
  compiler='scrooge',
  language='scala',
  rpc_style='finagle',
)
