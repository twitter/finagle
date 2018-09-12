java_library(
    sources = rglobs("*.java"),
    compiler_option_sets = {"fatal_warnings"},
    provides = artifact(
        org = "com.twitter",
        name = "finagle-mux-java",
        repo = artifactory,
    ),
    dependencies = [
        "finagle/finagle-mux/src/main/scala",
    ],
)
