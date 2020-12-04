java_thrift_library(
    name = "scribe",
    sources = ["scribe.thrift"],
    compiler = "scrooge",
    language = "scala",
    provides = artifact(
        org = "com.twitter",
        name = "finagle-thrift-scribe",
        repo = artifactory,
    ),
)

java_thrift_library(
    name = "tracing",
    sources = ["tracing.thrift"],
    compiler = "scrooge",
    language = "scala",
    provides = artifact(
        org = "com.twitter",
        name = "finagle-thrift-tracing",
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

java_thrift_library(
    # Renamed longer to assist bazel migration
    name = "thrift-bazel-workaround-java",
    sources = ["*.thrift"],
    compiler = "scrooge",
    language = "java",
)
