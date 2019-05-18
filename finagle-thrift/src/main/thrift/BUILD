java_thrift_library(
    sources = rglobs("*.thrift"),
    provides = artifact(
        org = "com.twitter",
        name = "finagle-thrift-thrift",
        repo = artifactory,
    ),
)

java_thrift_library(
    name = "thrift-scala",
    sources = globs("*.thrift"),
    compiler = "scrooge",
    language = "scala",
)
