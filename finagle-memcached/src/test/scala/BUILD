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
        "finagle/finagle-core/src/main",
        "finagle/finagle-memcached/src/main/scala",
        "util/util-core:scala",
    ],
)

junit_tests(
    name = "scala",
    sources = ["**/*.scala"] + exclude_globs(EXTERNAL_SOURCES),
    compiler_option_sets = ["fatal_warnings"],
    tags = ["known-jdk11-failure:VM-4210"],
    dependencies = [
        ":external",
        "3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind",
        "3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala",
        "3rdparty/jvm/junit",
        "3rdparty/jvm/org/mockito:mockito-all",
        "3rdparty/jvm/org/scalacheck",
        "3rdparty/jvm/org/scalatest",
        "3rdparty/jvm/org/scalatestplus:junit",
        "3rdparty/jvm/org/scalatestplus:mockito-1-10",
        "3rdparty/jvm/org/scalatestplus:scalacheck",
        "finagle/finagle-core/src/main",
        "finagle/finagle-core/src/test/scala:stringprotocol",
        "finagle/finagle-memcached/src/main/scala",
        "finagle/finagle-memcached/src/test/resources",
        "finagle/finagle-partitioning/src/main/scala",
        "finagle/finagle-serversets",
        "util/util-core:scala",
        "util/util-hashing",
        "util/util-registry/src/main/scala",
        "util/util-stats/src/main/scala",
        "util/util-zk-test",
    ],
)
