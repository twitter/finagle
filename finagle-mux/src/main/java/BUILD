java_library(
    sources = rglobs("*.java"),
    fatal_warnings = True,
    provides = artifact(
        org = "com.twitter",
        name = "finagle-mux-java",
        repo = artifactory,
    ),
    dependencies = [
        "finagle/finagle-mux/src/main/scala",
    ],
)
