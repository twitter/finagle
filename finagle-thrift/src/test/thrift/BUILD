java_thrift_library(
    name = "thrift-java",
    sources = ["echo.thrift"],
    compiler = "scrooge",
    language = "java",
)

java_thrift_library(
    name = "thrift-scala",
    sources = ["echo.thrift"],
    language = "scala",
)

java_thrift_library(
    name = "test-thrift-scala",
    sources = [
        "fb303.thrift",
        "test.thrift",
    ],
    compiler = "scrooge",
    language = "scala",
)

java_thrift_library(
    name = "test-thrift-java",
    sources = [
        "fb303.thrift",
        "test.thrift",
    ],
    compiler = "scrooge",
    language = "java",
)
