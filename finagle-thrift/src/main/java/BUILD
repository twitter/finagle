java_library(name='java',
  sources=rglobs('*.java'),
  dependencies=[
    '3rdparty:slf4j-api',
    '3rdparty:libthrift-0.5.0',
  ],
  provides=artifact(org='com.twitter',
                    name='finagle-thrift-java',
                    repo = artifactory,),
)
