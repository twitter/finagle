scala_library(
    sources = [
        "scala-2.12-/**/*.scala",
        "scala/**/*.scala",
    ],
    compiler_option_sets = ["fatal_warnings"],
    platform = "java8",
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-base-http",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/com/google/code/findbugs:jsr305",
        "3rdparty/jvm/io/netty:netty4",
        "3rdparty/jvm/io/netty:netty4-codec-http",
        "finagle/finagle-base-http/src/main/resources",
        "finagle/finagle-core",
        "finagle/finagle-toggle/src/main/scala",
        "util/util-app",
        "util/util-codec",
        "util/util-core",
        "util/util-stats",
    ],
    exports = [
        "util/util-core",
    ],
)
