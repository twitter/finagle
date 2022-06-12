SOURCES = ["**/*.scala"]

DEPENDENCIES = [
    "3rdparty/jvm/io/netty:netty4",
    "3rdparty/jvm/junit",
    "3rdparty/jvm/org/apache/thrift:libthrift",
    "3rdparty/jvm/org/mockito:mockito-all",
    "3rdparty/jvm/org/scalatest",
    "3rdparty/jvm/org/scalatestplus:junit",
    "3rdparty/jvm/org/scalatestplus:mockito-1-10",
    "finagle/finagle-core/src/main",
    "finagle/finagle-thrift/src/main/java",
    "finagle/finagle-thrift/src/test/resources",
    "finagle/finagle-thrift/src/test/thrift:delivery-thrift-java",
    "finagle/finagle-thrift/src/test/thrift:delivery-thrift-scala",
    "finagle/finagle-thrift/src/test/thrift:test-thrift-java",
    "finagle/finagle-thrift/src/test/thrift:test-thrift-scala",
    "finagle/finagle-thrift/src/test/thrift:thrift-java",
    "finagle/finagle-thrift/src/test/thrift:thrift-scala",
    "scrooge/scrooge-core/src/main/scala",
    "util/util-app/src/main/scala",
    "util/util-core:scala",
    "util/util-stats/src/main/scala",
]

COMPILER_OPTION_SETS = ["fatal_warnings"]

scala_library(
    name = "lib",
    sources = SOURCES,
    compiler_option_sets = COMPILER_OPTION_SETS,
    tags = ["bazel-compatible"],
    dependencies = DEPENDENCIES,
)

junit_tests(
    sources = SOURCES,
    compiler_option_sets = COMPILER_OPTION_SETS,
    tags = [
        "bazel-compatible",
        "known-to-fail-jira:CSL-12083",
    ],
    dependencies = DEPENDENCIES,
)
