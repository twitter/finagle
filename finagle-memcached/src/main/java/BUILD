java_library(
    sources = rglobs("*.java"),
    fatal_warnings = True,
    provides = artifact(
        org = "com.twitter",
        name = "finagle-memcached-java",
        repo = artifactory,
    ),
    dependencies = [
        "3rdparty/jvm/com/google/guava",
        "3rdparty/jvm/com/google/inject:guice",
        "3rdparty/jvm/org/scala-lang:scala-library",
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-memcached/src/main/scala",
        "util/util-core/src/main/scala",
        "util/util-function/src/main/java",
        "util/util-stats/src/main/scala",
    ],
)
