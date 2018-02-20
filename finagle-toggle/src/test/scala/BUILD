junit_tests(
    sources = rglobs("*.scala"),
    fatal_warnings = True,
    dependencies = [
        "3rdparty/jvm/com/fasterxml/jackson/core:jackson-annotations",
        "3rdparty/jvm/com/fasterxml/jackson/core:jackson-core",
        "3rdparty/jvm/junit",
        "3rdparty/jvm/org/scalacheck",
        "3rdparty/jvm/org/scalatest",
        "finagle/finagle-toggle",
        "finagle/finagle-toggle/src/test/resources",
        "util/util-app/src/main/java",
        scoped(
            "util/util-app/src/main/scala",
            scope = "forced",
        ),
        "util/util-core/src/main/scala",
        "util/util-logging",
        "util/util-stats",
    ],
)
