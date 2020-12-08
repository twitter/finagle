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

create_thrift_libraries(
    # Renamed longer to assist bazel migration
    base_name = "thrift-bazel-workaround",
    sources = ["*.thrift"],
    generate_languages = [
        "java",
        "scala",
    ],
)
