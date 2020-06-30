create_thrift_libraries(
    base_name = "thrift",
    sources = ["echo.thrift"],
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
    generate_languages = [
        "java",
        "scala",
    ],
)
