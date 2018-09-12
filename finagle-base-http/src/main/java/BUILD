java_library(
    sources = rglobs("*.java"),
    compiler_option_sets = {"fatal_warnings"},
    provides = artifact(
        org = "com.twitter",
        name = "finagle-http-java",
        repo = artifactory,
    ),
    dependencies = [
        "finagle/finagle-base-http/src/main/scala",
    ],
)
