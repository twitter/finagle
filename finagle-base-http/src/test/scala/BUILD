junit_tests(
    sources = ["**/*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/io/netty:netty4",
        "3rdparty/jvm/io/netty:netty4-codec-http",
        "3rdparty/jvm/junit",
        "3rdparty/jvm/org/mockito:mockito-core",
        "3rdparty/jvm/org/scalacheck",
        "3rdparty/jvm/org/scalatest",
        "3rdparty/jvm/org/scalatestplus:junit",
        "3rdparty/jvm/org/scalatestplus:mockito",
        "3rdparty/jvm/org/scalatestplus:scalacheck",
        "finagle/finagle-base-http",
        "finagle/finagle-base-http/src/test/resources",
        "finagle/finagle-core/src/main",
        "finagle/finagle-toggle/src/main/scala",
    ],
)

scala_library(
    name = "cookie-map-test-lib",
    sources = ["com/twitter/finagle/http/CookieMapTest.scala"],
    compiler_option_sets = ["fatal_warnings"],
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/org/scalatest",
        "finagle/finagle-base-http",
    ],
)
