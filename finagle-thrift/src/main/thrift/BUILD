java_thrift_library(
    sources = ["**/*.thrift"],
    provides = artifact(
        org = "com.twitter",
        name = "finagle-thrift-thrift",
        repo = artifactory,
    ),
)

java_thrift_library(
    name = "thrift-scala",
    sources = ["*.thrift"],
    compiler = "scrooge",
    language = "scala",
)
