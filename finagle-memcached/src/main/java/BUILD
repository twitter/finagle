java_library(
    sources = ["**/*.java"],
    compiler_option_sets = ["fatal_warnings"],
    provides = artifact(
        org = "com.twitter",
        name = "finagle-memcached-java",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/com/google/guava",
        "3rdparty/jvm/com/google/inject:guice",
        "3rdparty/jvm/org/scala-lang:scala-library",
        "finagle/finagle-core/src/main",
        "finagle/finagle-memcached/src/main/scala",
        "util/util-core:scala",
        "util/util-function/src/main/java",
        "util/util-stats/src/main/scala",
    ],
)

# TODO: Change this to filegroup per https://github.com/pantsbuild/pants/issues/7200
# when we switch entirely to bazel
files(
    name = "pants-workaround",
    sources = ["**/*.java"],
    tags = ["bazel-compatible"],
)
