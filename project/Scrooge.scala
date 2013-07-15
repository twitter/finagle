
import scala.collection.JavaConverters._

import sbt._
import Keys._

object Scrooge extends Plugin {
  // keys used to fetch scrooge2:
  val scrooge2Version = SettingKey[String](
    "scrooge2-version",
    "version of scrooge2 to download and use"
  )

  val scrooge2Name = SettingKey[String](
    "scrooge2-name",
    "scrooge2's version-qualified name ('scrooge2-' + scrooge2-version)"
  )

  val scrooge2CacheFolder = SettingKey[File](
    "scrooge2-cache-folder",
    "where to unpack the downloaded scrooge2 package"
  )

  val scrooge2Jar = SettingKey[File](
    "scrooge2-jar",
    "the local scrooge2 jar file"
  )

  val scrooge2Fetch = TaskKey[File](
    "scrooge2-fetch",
    "fetch the scrooge2 zip package and unpack it into scrooge2-cache-folder"
  )

  // keys used for actual scrooge2 generation:
  val scrooge2BuildOptions = SettingKey[Seq[String]](
    "scrooge2-build-options",
    "command line args to pass to scrooge2"
  )

  val scrooge2ThriftIncludeFolders = SettingKey[Seq[File]](
    "scrooge2-thrift-include-folders",
    "folders to search for thrift 'include' directives"
  )

  val scrooge2ThriftNamespaceMap = SettingKey[Map[String, String]](
    "scrooge2-thrift-namespace-map",
    "namespace rewriting, to support generation of java/finagle/scrooge2 into the same jar"
  )

  val scrooge2ThriftSourceFolder = SettingKey[File](
    "scrooge2-thrift-source-folder",
    "directory containing thrift source files"
  )

  val scrooge2ThriftSources = SettingKey[Seq[File]](
    "scrooge2-thrift-sources",
    "thrift source files to compile"
  )

  val scrooge2ThriftOutputFolder = SettingKey[File](
    "scrooge2-thrift-output-folder",
    "output folder for generated scala files (defaults to sourceManaged)"
  )

  val scrooge2IsDirty = TaskKey[Boolean](
    "scrooge2-is-dirty",
    "true if scrooge2 has decided it needs to regenerate the scala files from thrift sources"
  )

  val scrooge2Gen = TaskKey[Seq[File]](
    "scrooge2-gen",
    "generate scala code from thrift files using scrooge2"
  )

  /**
   * these settings will go into both the compile and test configurations.
   * you can add them to other configurations by using inConfig(<config>)(genThriftSettings),
   * e.g. inConfig(Assembly)(genThriftSettings)
   */
  val genThriftSettings: Seq[Setting[_]] = Seq(
    scrooge2ThriftSourceFolder <<= (sourceDirectory) { _ / "thrift" },
    scrooge2ThriftSources <<= (scrooge2ThriftSourceFolder) { srcDir => (srcDir ** "*.thrift").get },
    scrooge2ThriftOutputFolder <<= (sourceManaged) { x => x },
    scrooge2ThriftIncludeFolders := Seq(),
    scrooge2ThriftNamespaceMap := Map(),

    // look at includes and our sources to see if anything is newer than any of our output files
    scrooge2IsDirty <<= (
      streams,
      scrooge2ThriftSources,
      scrooge2ThriftOutputFolder,
      scrooge2ThriftIncludeFolders
    ) map { (out, sources, outputDir, inc) =>
      // figure out if we need to actually rebuild, based on mtimes.
      val allSourceDeps = sources ++ inc.foldLeft(Seq[File]()) { (files, dir) =>
        files ++ (dir ** "*.thrift").get
      }
      val sourcesLastModified:Seq[Long] = allSourceDeps.map(_.lastModified)
      val newestSource = if (sourcesLastModified.size > 0) {
        sourcesLastModified.max
      } else {
        Long.MaxValue
      }
      val outputsLastModified = (outputDir ** "*.scala").get.map(_.lastModified)
      val oldestOutput = if (outputsLastModified.size > 0) {
        outputsLastModified.min
      } else {
        Long.MinValue
      }
      oldestOutput < newestSource
    },

    // actually run scrooge2
    scrooge2Gen <<= (
      streams,
      scrooge2IsDirty,
      scrooge2ThriftSources,
      scrooge2ThriftOutputFolder,
      scrooge2Fetch,
      scrooge2BuildOptions,
      scrooge2ThriftIncludeFolders,
      scrooge2ThriftNamespaceMap
    ) map { (out, isDirty, sources, outputDir, jar, opts, inc, ns) =>
      // for some reason, sbt sometimes calls us multiple times, often with no source files.
      outputDir.mkdirs()
      if (isDirty && !sources.isEmpty) {
        out.log.info("Generating scrooge2 thrift for %s ...".format(sources.mkString(", ")))
        val sourcePaths = sources.mkString(" ")
        val namespaceMappings = ns.map { case (k, v) =>
          "-n " + k + "=" + v
        }.mkString(" ")
        val thriftIncludes = inc.map { folder =>
          "-i " + folder.getAbsolutePath
        }.mkString(" ")
        val cmd = "java -jar %s %s %s %s -d %s -s %s".format(
          jar, opts.mkString(" "), thriftIncludes, namespaceMappings,
          outputDir.getAbsolutePath, sources.mkString(" "))
        out.log.debug(cmd)
        cmd ! out.log
      }
      (outputDir ** "*.scala").get.toSeq
    },
    sourceGenerators <+= scrooge2Gen
  )

  val newSettings = Seq(
    scrooge2Version := "2.6.1",
    scrooge2BuildOptions := Seq("--finagle"),
    scrooge2Name <<= (scrooge2Version) { ver => "scrooge-%s".format(ver) },
    scrooge2CacheFolder <<= (baseDirectory) { (base) =>
      base / "project" / "target"
    },
    scrooge2Jar <<= (scrooge2CacheFolder, scrooge2Name) { (folder, name) =>
      folder / (name + ".jar")
    },

    scrooge2Fetch <<= (
      streams,
      scrooge2CacheFolder,
      scrooge2Jar,
      scrooge2Version
    ) map { (out, cacheFolder, jar, ver) =>
      if (!jar.exists) {
        out.log.info("Fetching scrooge2 " + ver + " ...")

        val environment = System.getenv().asScala
        val homeRepo =  "http://maven.twttr.com/"
        val localRepo = System.getProperty("user.home") + "/.m2/repository/"
        val zipPath = "/com/twitter/scrooge/" + ver + "/scrooge-" + ver + ".zip"
        val fetchUrl = if (new File(localRepo + zipPath).exists) {
          "file:" + localRepo + zipPath
        } else {
          homeRepo + zipPath
        }
        out.log.info("Fetching from: " + fetchUrl)

        cacheFolder.asFile.mkdirs()
        IO.unzipURL(new URL(fetchUrl), cacheFolder)
        if (jar.exists) {
          jar
        } else {
          error("failed to fetch and unpack %s at %s".format(fetchUrl, jar))
        }
      } else {
        jar
      }
    }
  ) ++ inConfig(Test)(genThriftSettings) ++ inConfig(Compile)(genThriftSettings)
}

