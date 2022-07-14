java_thrift_library(
    name = "scribe",
    sources = ["scribe.thrift"],
    compiler = "scrooge",
    language = "scala",
    platform = "java8",
    provides = artifact(
        org = "com.twitter",
        name = "finagle-thrift-scribe",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
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
    tags = ["bazel-compatible"],
)

create_thrift_libraries(
    # Renamed longer to assist bazel migration
    base_name = "thrift-bazel-workaround",
    sources = ["*.thrift"],
    platform = "java8",
    tags = ["bazel-compatible"],
    generate_languages = [
        "java",
        "scala",
    ],
)
