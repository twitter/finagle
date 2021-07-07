target(
    tags = ["bazel-compatible"],
    dependencies = [
        "finagle/finagle-mysql/src/main/scala",
    ],
)

test_suite(
    name = "tests",
    dependencies = [
        "finagle/finagle-mysql/src/test/java",
        "finagle/finagle-mysql/src/test/scala/com/twitter/finagle/mysql/integration",
        "finagle/finagle-mysql/src/test/scala/com/twitter/finagle/mysql/unit",
    ],
)
