java_thrift_library(
    name = "thrift-scala",
    sources = rglobs("*.thrift"),
    compiler = "scrooge",
    language = "scala",
    provides = artifact(
        org = "com.twitter",
        name = "finagle-example-thrift-scala",
        repo = artifactory,
    ),
)

java_thrift_library(
    name = "thrift-java",
    sources = rglobs("*.thrift"),
    compiler = "scrooge",
    language = "java",
    provides = artifact(
        org = "com.twitter",
        name = "finagle-example-thrift-java",
        repo = artifactory,
    ),
)
