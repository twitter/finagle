java_library(
    sources = rglobs("*.java"),
    fatal_warnings = True,
    provides = artifact(
        org = "com.twitter",
        name = "finagle-http-java",
        repo = artifactory,
    ),
    dependencies = [
        "finagle/finagle-base-http/src/main/scala",
    ],
)
