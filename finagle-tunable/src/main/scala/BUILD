scala_library(
    sources = ["**/*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    platform = "java8",
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-tunable",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala",
        "finagle/finagle-toggle/src/main/scala/com/twitter/finagle/server",
        "util/util-core:scala",
        "util/util-tunable/src/main/scala",
    ],
    exports = [
    ],
)
