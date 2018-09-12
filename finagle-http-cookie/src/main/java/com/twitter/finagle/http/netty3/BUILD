java_library(
    sources = rglobs("*.java"),
    compiler_option_sets = {"fatal_warnings"},
    provides = artifact(
        org = "com.twitter",
        name = "finagle-http-cookie",
        repo = artifactory,
    ),
    dependencies = [
        "3rdparty/jvm/io/netty",
    ],
)
