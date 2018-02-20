java_library(
    sources = rglobs("*.java"),
    fatal_warnings = True,
    provides = artifact(
        org = "com.twitter",
        name = "finagle-core-java",
        repo = artifactory,
    ),
    dependencies = [
        "finagle/finagle-core/src/main/scala",
        "util/util-core/src/main/java",
        "util/util-core/src/main/scala",
    ],
)
