scala_library(
    sources = rglobs("*.scala"),
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-redis",
        repo = artifactory,
    ),
    dependencies = [
        "finagle/finagle-core",
        "finagle/finagle-netty4",
        "util/util-core",
        "util/util-logging",
        "util/util-stats",
    ],
    exports = [
        "finagle/finagle-core",
        "util/util-core",
    ],
)
