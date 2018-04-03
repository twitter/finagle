scala_library(
    name = "integration-client",
    sources = [
        "com/twitter/finagle/mysql/integration/IntegrationClient.scala",
    ],
    fatal_warnings = True,
    dependencies = [
        "3rdparty/jvm/org/scalatest",
        "finagle/finagle-mysql/src/main/scala",
    ],
)

junit_tests(
    sources = rglobs("*.scala"),
    fatal_warnings = True,
    dependencies = [
        "3rdparty/jvm/com/github/ben-manes/caffeine",
        "3rdparty/jvm/io/netty",
        "3rdparty/jvm/junit",
        "3rdparty/jvm/mysql:mysql-connector-java",
        "3rdparty/jvm/mysql:mysql-connector-mxj-gpl-db-files",
        "3rdparty/jvm/org/mockito:mockito-all",
        "3rdparty/jvm/org/scalatest",
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-mysql",
        "finagle/finagle-mysql/src/test/resources",
        "util/util-core/src/main/scala",
    ],
)
