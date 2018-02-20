EXTERNAL_SOURCES = [
    "com/twitter/finagle/memcached/integration/external/ExternalMemcached.scala",
    "com/twitter/finagle/memcached/integration/external/InProcessMemcached.scala",
]

scala_library(
    name = "external",
    sources = EXTERNAL_SOURCES,
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-memcached-testutils",
        repo = artifactory,
    ),
    dependencies = [
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-memcached/src/main/scala",
        "util/util-collection",
        "util/util-core/src/main/scala",
    ],
)

junit_tests(
    sources = rglobs(
        "*.scala",
        exclude = [EXTERNAL_SOURCES],
    ),
    fatal_warnings = True,
    dependencies = [
        ":external",
        "3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind",
        "3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala",
        "3rdparty/jvm/io/netty",
        "3rdparty/jvm/junit",
        "3rdparty/jvm/org/apache/zookeeper:zookeeper-server",
        "3rdparty/jvm/org/mockito:mockito-all",
        "3rdparty/jvm/org/scalacheck",
        "3rdparty/jvm/org/scalatest",
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-core/src/test/scala",
        "finagle/finagle-memcached/src/main/scala",
        "finagle/finagle-memcached/src/test/resources",
        "finagle/finagle-serversets",
        "finagle/finagle-toggle/src/main/scala",
        "util/util-collection/src/main/scala",
        "util/util-core/src/main/scala",
        "util/util-hashing",
        "util/util-registry/src/main/scala",
        "util/util-stats/src/main/scala",
        "util/util-zk-test",
    ],
)
