java_library(
    compiler_option_sets = ["fatal_warnings"],
    provides = artifact(
        org = "com.twitter",
        name = "finagle-grpc-context",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/io/grpc:grpc-context",
        "util/util-core:util-core-util",
    ],
)
