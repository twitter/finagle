scala_library(
    sources = rglobs("*.scala"),
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-mysql",
        repo = artifactory,
    ),
    dependencies = [
        "3rdparty/jvm/com/github/ben-manes/caffeine",
        "3rdparty/jvm/com/google/code/findbugs:jsr305",
        "3rdparty/jvm/io/netty",
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-mysql/src/main/resources",
        "finagle/finagle-netty4",
        "finagle/finagle-toggle",
        "util/util-cache",
        "util/util-core/src/main/scala",
        "util/util-logging/src/main/scala",
        "util/util-stats/src/main/scala",
    ],
    exports = [
        "util/util-core/src/main/scala",
    ],
)
