java_library(
    sources = rglobs("*.java"),
    compiler_option_sets = {"fatal_warnings"},
    zinc_file_manager = False,  # java.lang.IllegalArgumentException is thrown without this:
    dependencies = [
        "3rdparty/jvm/org/hdrhistogram",
        "3rdparty/jvm/org/openjdk/jmh:jmh-core",
        "3rdparty/jvm/org/openjdk/jmh:jmh-generator-annprocess",
        "finagle/finagle-benchmark/src/main/scala",
        "finagle/finagle-core/src/main/scala",
        "util/util-core/src/main/scala",
    ],
    # Not supported: __shaded_by_pants__.sbt.internal.inc.javac.WriteReportingJavaFileObject@3ea7d550
)

jvm_binary(
    name = "jmh",
    main = "org.openjdk.jmh.Main",
    dependencies = [
        ":util",
    ],
)
