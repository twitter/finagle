java_library(
    sources = ["**/*.java"],
    compiler_option_sets = ["fatal_warnings"],
    provides = artifact(
        org = "com.twitter",
        name = "finagle-netty4-java",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/com/github/luben:zstd-jni",
        "3rdparty/jvm/io/netty:netty4",
        "3rdparty/jvm/io/netty:netty4-codec",
    ],
)
