java_thrift_library(
  name='thrift-java',
  language='java',
  compiler='scrooge',
  sources=['echo.thrift'],
)
java_thrift_library(
  name='thrift-scala',
  language='scala',
  sources=['echo.thrift'],
)

java_thrift_library(name='test-thrift-scala',
  sources=['test.thrift', 'fb303.thrift'],
  compiler='scrooge',
  language='scala',
)
java_thrift_library(name='test-thrift-java',
  sources=['test.thrift', 'fb303.thrift'],
  compiler='scrooge',
  language='java',
)
