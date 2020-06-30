java_thrift_library(
    sources = ["**/*.thrift"],
    provides = artifact(
        org = "com.twitter",
        name = "finagle-thrift-thrift",
        repo = artifactory,
    ),
)

java_thrift_library(
    # Renamed longer to assist bazel migration
    name = "thrift-bazel-workaround-scala",
    sources = ["*.thrift"],
    compiler = "scrooge",
    language = "scala",
)
