scala_library(
    sources = ["**/*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-http",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/com/google/code/findbugs:jsr305",
        "finagle/finagle-base-http",
        "finagle/finagle-core",
        "finagle/finagle-http2",
        "finagle/finagle-netty4",
        "finagle/finagle-netty4-http",
        "finagle/finagle-toggle",
        scoped(
            "util/util-app",
            scope = "forced",
        ),
        "util/util-codec",
        "util/util-core",
        "util/util-logging",
        "util/util-routing/src/main/scala/com/twitter/util/routing",
        "util/util-stats",
    ],
    exports = [
        "finagle/finagle-base-http",
        "finagle/finagle-core",
        "util/util-core",
    ],
)
