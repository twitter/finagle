java_thrift_library(
    name = "thrift-scala",
    sources = ["**/*.thrift"],
    compiler = "scrooge",
    language = "scala",
    provides = artifact(
        org = "com.twitter",
        name = "finagle-benchmark",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
)
