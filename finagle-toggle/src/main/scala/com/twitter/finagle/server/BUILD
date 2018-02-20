scala_library(
    sources = rglobs("*.scala"),
    fatal_warnings = True,
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-server",
        repo = artifactory,
    ),
    dependencies = [
        "util/util-core/src/main/scala",
        "util/util-registry/src/main/scala",
    ],
)
