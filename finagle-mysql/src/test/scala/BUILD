jar_library(name = 'mysql-connector-mxj',
  dependencies = [
    jar(org = 'mysql', name = 'mysql-connector-mxj-gpl', rev = '5.0.11')
  ]
)

jar_library(name = 'mysql-connector-mxj-db-files',
  dependencies = [
    jar(org = 'mysql', name = 'mysql-connector-mxj-gpl-db-files', rev = '5.0.11')
  ]
)

jar_library(name = 'mysql-connector-java',
  dependencies = [
    jar(org = 'mysql', name = 'mysql-connector-java', rev = '5.1.25')
  ]
)

scala_library(name='testing',
  dependencies=[
    pants('finagle/finagle-mysql'),
    pants(':mysql-connector-mxj'),
    pants(':mysql-connector-mxj-db-files'),
    pants(':mysql-connector-java')
  ],
  sources=rglobs('*.scala')
)

junit_tests(name='scala',
  dependencies=[
    pants('3rdparty:junit'),
    pants('3rdparty:mockito-all'),
    pants('3rdparty:netty'),
    pants('3rdparty:specs'),
    pants('3rdparty:scalatest'),
    pants('finagle/finagle-mysql'),
    pants(':mysql-connector-mxj'),
    pants(':mysql-connector-mxj-db-files'),
    pants(':mysql-connector-java'),
    pants('util/util-core')
  ],
  sources=rglobs('*.scala'),
  resources=[
    pants('finagle/finagle-mysql/src/test/resources')
  ]
)
