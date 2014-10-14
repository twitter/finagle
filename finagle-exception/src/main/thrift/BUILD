java_thrift_library(name='thrift-scala',
  sources=rglobs('*.thrift'),
  compiler='scrooge',
  language='scala',
  rpc_style='finagle',
  provides=artifact(org='com.twitter',
                    name='finagle-exception-thrift',
                    repo = artifactory,),
)
