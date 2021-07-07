scala_library(
    sources = ["**/*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-mysql",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/com/fasterxml/jackson/core:jackson-core",
        "3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind",
        "3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala",
        "3rdparty/jvm/com/github/ben-manes/caffeine",
        "3rdparty/jvm/com/google/code/findbugs:jsr305",
        "finagle/finagle-core/src/main",
        "finagle/finagle-mysql/src/main/resources",
        "finagle/finagle-netty4",
        "finagle/finagle-toggle",
        "util/util-cache",
        "util/util-core:scala",
        "util/util-logging/src/main/scala",
        "util/util-stats/src/main/scala",
    ],
    exports = [
        "util/util-core:scala",
    ],
)
