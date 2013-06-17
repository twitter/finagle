thrift_library(name='thrift',
  sources=rglobs('*.thrift')
)

compiled_idl(name='compiled',
  idl_deps=pants(':thrift'),
  compiler='scrooge-legacy',
  language='scala',
  rpc_style='finagle'
)
