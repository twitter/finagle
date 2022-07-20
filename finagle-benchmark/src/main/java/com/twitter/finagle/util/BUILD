java_library(
    sources = ["**/*.java"],
    compiler_option_sets = ["fatal_warnings"],
    platform = "java8",
    tags = ["bazel-compatible"],
    zinc_file_manager = False,  # java.lang.IllegalArgumentException is thrown without this:
    dependencies = [
        "3rdparty/jvm/org/hdrhistogram",
        "3rdparty/jvm/org/openjdk/jmh:jmh-core",
        "3rdparty/jvm/org/openjdk/jmh:jmh-generator-annprocess",
        "finagle/finagle-benchmark/src/main/scala",
        "finagle/finagle-core/src/main",
        "util/util-core:scala",
    ],
)

jvm_binary(
    name = "jmh",
    main = "org.openjdk.jmh.Main",
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        ":util",
    ],
)
