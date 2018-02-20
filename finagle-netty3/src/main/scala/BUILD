scala_library(
    sources = rglobs("*.scala"),
    fatal_warnings = True,
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-netty3",
        repo = artifactory,
    ),
    dependencies = [
        "3rdparty/jvm/io/netty",
        "finagle/finagle-core/src/main/scala",
        "util/util-app/src/main/scala",
        "util/util-codec/src/main/scala",
        "util/util-core/src/main/scala",
        "util/util-jvm/src/main/scala",
        "util/util-lint/src/main/scala",
        "util/util-logging/src/main/scala",
        "util/util-security/src/main/scala",
        "util/util-stats/src/main/scala",
    ],
    exports = [
        "3rdparty/jvm/io/netty",
        "finagle/finagle-core/src/main/scala",
        "util/util-core/src/main/scala",
        "util/util-stats/src/main/scala",
    ],
)
