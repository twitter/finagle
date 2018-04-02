junit_tests(
    sources = rglobs("*.scala"),
    fatal_warnings = True,
    dependencies = [
        scoped(
            "3rdparty/jvm/io/netty",
            scope = "forced",
        ),
        "3rdparty/jvm/io/netty:netty4-codec-http",
        "3rdparty/jvm/io/netty:netty4-codec-http2",
        "3rdparty/jvm/io/netty:netty4-handler-proxy",
        "3rdparty/jvm/junit",
        "3rdparty/jvm/org/mockito:mockito-all",
        "3rdparty/jvm/org/scalatest",
        "finagle/finagle-base-http",
        "finagle/finagle-core",
        "finagle/finagle-http2",
        "finagle/finagle-netty4",
        "finagle/finagle-netty4-http",
        "util/util-core",
        "util/util-logging",
        "util/util-stats",
    ],
)
