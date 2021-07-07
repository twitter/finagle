create_thrift_libraries(
    base_name = "thrift",
    sources = ["**/*.thrift"],
    tags = ["bazel-compatible"],
    generate_languages = [
        "java",
        "scala",
    ],
    provides_java_name = "finagle-example-thrift-java",
    provides_scala_name = "finagle-example-thrift-scala",
)
