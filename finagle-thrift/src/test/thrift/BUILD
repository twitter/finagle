create_thrift_libraries(
    base_name = "thrift",
    sources = ["echo.thrift"],
    tags = ["bazel-compatible"],
    generate_languages = [
        "java",
        "scala",
    ],
)

create_thrift_libraries(
    base_name = "test-thrift",
    sources = [
        "fb303.thrift",
        "test.thrift",
    ],
    tags = ["bazel-compatible"],
    generate_languages = [
        "java",
        "scala",
    ],
)

create_thrift_libraries(
    base_name = "delivery-thrift",
    sources = [
        "delivery_service.thrift",
    ],
    tags = ["bazel-compatible"],
    generate_languages = [
        "java",
        "scala",
    ],
)
