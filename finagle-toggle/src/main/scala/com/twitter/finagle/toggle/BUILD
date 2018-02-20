scala_library(
    sources = rglobs("*.scala"),
    fatal_warnings = True,
    dependencies = [
        "3rdparty/jvm/com/fasterxml/jackson/core:jackson-annotations",
        "3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind",
        "3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala",
        "finagle/finagle-toggle/src/main/scala/com/twitter/finagle/server",
        "util/util-app/src/main/java",
        "util/util-app/src/main/scala",
        "util/util-core/src/main/scala",
        "util/util-logging",
        "util/util-registry/src/main/scala",
        "util/util-stats",
    ],
)
