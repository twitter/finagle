java_library(
    sources = ["**/*.java"],
    compiler_option_sets = ["fatal_warnings"],
    provides = artifact(
        org = "com.twitter",
        name = "finagle-mux-java",
        repo = artifactory,
    ),
    dependencies = [
        "finagle/finagle-core/src/main",
    ],
)
