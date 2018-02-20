scala_library(
    sources = rglobs("*.scala"),
    fatal_warnings = True,
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-tunable",
        repo = artifactory,
    ),
    dependencies = [
        "3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala",
        "finagle/finagle-toggle/src/main/scala/com/twitter/finagle/server",
        "util/util-core/src/main/scala",
        "util/util-tunable/src/main/scala",
    ],
    exports = [
    ],
)
