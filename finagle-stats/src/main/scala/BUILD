scala_library(
    sources = rglobs("*.scala"),
    fatal_warnings = True,
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-stats",
        repo = artifactory,
    ),
    # In the vast majority of cases, the consumers of finagle-stats only need it at runtime,
    # and so ideally we would mark this "scope=runtime". But there are enough consumers that also
    # need it at compiletime that it is non-trivial to split the target at this juncture.
    scope = "forced",
    dependencies = [
        "3rdparty/jvm/com/fasterxml/jackson/core:jackson-annotations",
        "3rdparty/jvm/com/fasterxml/jackson/core:jackson-core",
        "3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind",
        "3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala",
        "finagle/finagle-base-http/src/main/scala",
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-http/src/main/scala",
        "finagle/finagle-stats/src/main/resources",
        "finagle/finagle-tunable/src/main/scala",
        "util/util-app/src/main/scala",
        "util/util-core/src/main/scala",
        "util/util-lint/src/main/scala",
        "util/util-logging",
        "util/util-registry",
        "util/util-stats",
        "util/util-tunable/src/main/scala",
    ],
    exports = [
        "finagle/finagle-core/src/main/scala",
        "util/util-stats",
    ],
)
