java_library(
    sources = ["**/*.java"],
    platform = "java8",
    provides = artifact(
        org = "com.twitter",
        name = "finagle-thrift-java",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/javax/annotation:javax.annotation-api",
        "3rdparty/jvm/org/apache/thrift:libthrift",
    ],
)

# For usecases (like scrooge codegen) where this dependency is used, but not detected as
# used by zinc.
target(
    name = "forced",
    scope = "forced",
    tags = ["bazel-compatible"],
    dependencies = [":java"],
)
