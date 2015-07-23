java_library(name='java',
  sources=rglobs('*.java'),
  dependencies=[
    '3rdparty/jvm/org/slf4j:slf4j-api',
    '3rdparty/jvm/org/apache/thrift:libthrift-0.5.0',
    'scrooge/scrooge-core',
  ],
  provides=artifact(org='com.twitter',
                    name='finagle-thrift-java',
                    repo = artifactory,),
)
