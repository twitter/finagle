java_library(
    sources = rglobs("*.java"),
    fatal_warnings = True,
    provides = artifact(
        org = "com.twitter",
        name = "finagle-http-cookie",
        repo = artifactory,
    ),
    dependencies = [
        "3rdparty/jvm/io/netty",
    ],
)
