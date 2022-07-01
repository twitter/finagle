java_library(
    sources = ["**/*.java"],
    platform = "java8",
    provides = artifact(
        org = "com.twitter.finagle.common",
        name = "finagle-serversets-java",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/com/github/ben-manes/caffeine",
        "3rdparty/jvm/com/google/code/findbugs:jsr305",
        "3rdparty/jvm/com/google/code/gson",
        "3rdparty/jvm/commons-lang",
        "3rdparty/jvm/org/apache/thrift:libthrift",
        "3rdparty/jvm/org/apache/zookeeper:zookeeper-client",
        "finagle/finagle-serversets/src/main/thrift:thrift-java",
        "util/util-core",
    ],
    exports = [
        "3rdparty/jvm/com/google/code/findbugs:jsr305",
        "finagle/finagle-serversets/src/main/thrift:thrift-java",
    ],
)
