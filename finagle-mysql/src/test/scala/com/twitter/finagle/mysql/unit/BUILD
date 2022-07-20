junit_tests(
    sources = [
        "**/*.scala",
    ],
    compiler_option_sets = ["fatal_warnings"],
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala",
        "3rdparty/jvm/com/github/ben-manes/caffeine",
        "3rdparty/jvm/junit",
        "3rdparty/jvm/mysql:mysql-connector-java",
        "3rdparty/jvm/org/mockito:mockito-all",
        "3rdparty/jvm/org/scalatest",
        "3rdparty/jvm/org/scalatestplus:junit",
        "3rdparty/jvm/org/scalatestplus:mockito-1-10",
        scoped(
            "3rdparty/jvm/mysql:mysql-connector-mxj-gpl-db-files",
            scope = "runtime",
        ),
        "finagle/finagle-core/src/main",
        "finagle/finagle-mysql",
        "finagle/finagle-mysql/src/test/resources",
        "finagle/finagle-mysql/src/test/scala/com/twitter/finagle/mysql/harness",
        "finagle/finagle-mysql/src/test/scala/com/twitter/finagle/mysql/harness/config",
        "util/util-core:scala",
    ],
)
