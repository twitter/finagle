thrift_library(name='thrift',
  sources=rglobs('*.thrift')
)

compiled_idl(name='scrooge2',
  idl_deps=pants(':thrift'),
  compiler='scrooge-legacy',
  language='scala',
  rpc_style='finagle',
  namespace_map={
    'com.twitter.finagle.thriftmux.thrift': 'com.twitter.finagle.thriftmux.thriftscrooge2'
  },
)

compiled_idl(name='scrooge3',
  idl_deps=pants(':thrift'),
  compiler='scrooge',
  language='scala',
  rpc_style='finagle',
  namespace_map={
    'com.twitter.finagle.thriftmux.thrift': 'com.twitter.finagle.thriftmux.thriftscrooge3'
  },
)
