junit_tests(
  dependencies=[
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-annotations',
    '3rdparty/jvm/junit',
    '3rdparty/jvm/org/scalacheck',
    '3rdparty/jvm/org/scalatest',
    'finagle/finagle-toggle',
    'finagle/finagle-toggle/src/test/resources',
    'util/util-app/src/main/java',
    scoped('util/util-app/src/main/scala', scope='forced'),
    'util/util-logging',
    'util/util-stats',
    'util/util-core/src/main/scala:scala',
  ],
  sources=rglobs('*.scala'),
  fatal_warnings=True,
  strict_deps=True,
)
