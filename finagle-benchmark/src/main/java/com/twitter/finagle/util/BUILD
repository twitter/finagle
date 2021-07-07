java_library(
    sources = ["**/*.java"],
    compiler_option_sets = ["fatal_warnings"],
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
    # Not supported: __shaded_by_pants__.sbt.internal.inc.javac.WriteReportingJavaFileObject@3ea7d550
)

jvm_binary(
    name = "jmh",
    main = "org.openjdk.jmh.Main",
    tags = ["bazel-compatible"],
    dependencies = [
        ":util",
    ],
)
