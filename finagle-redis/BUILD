target(
    tags = ["bazel-compatible"],
    dependencies = [
        "finagle/finagle-redis/src/main/scala",
    ],
)

test_suite(
    name = "tests",
    tags = ["bazel-compatible"],
    dependencies = [
        "finagle/finagle-redis/src/test/scala",
    ],
)

test_suite(
    name = "integration-tests",
    tags = ["bazel-compatible"],
    dependencies = [
        "finagle/finagle-redis/src/it/scala",
    ],
)
