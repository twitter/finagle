
import scala.collection.JavaConverters._

import sbt._
import Keys._

object Scrooge extends Plugin {
  // keys used to fetch scrooge:
  val scroogeVersion = SettingKey[String](
    "scrooge-version",
    "version of scrooge to download and use"
  )

  val scroogeName = SettingKey[String](
    "scrooge-name",
    "scrooge's version-qualified name ('scrooge-' + scrooge-version)"
  )

  val scroogeCacheFolder = SettingKey[File](
    "scrooge-cache-folder",
    "where to unpack the downloaded scrooge package"
  )

  val scroogeJar = SettingKey[File](
    "scrooge-jar",
    "the local scrooge jar file"
  )

  val scroogeFetch = TaskKey[File](
    "scrooge-fetch",
    "fetch the scrooge zip package and unpack it into scrooge-cache-folder"
  )

  // keys used for actual scrooge generation:
  val scroogeBuildOptions = SettingKey[Seq[String]](
    "scrooge-build-options",
    "command line args to pass to scrooge"
  )

  val scroogeThriftIncludeFolders = SettingKey[Seq[File]](
    "scrooge-thrift-include-folders",
    "folders to search for thrift 'include' directives"
  )

  val scroogeThriftNamespaceMap = SettingKey[Map[String, String]](
    "scrooge-thrift-namespace-map",
    "namespace rewriting, to support generation of java/finagle/scrooge into the same jar"
  )

  val scroogeThriftSourceFolder = SettingKey[File](
    "scrooge-thrift-source-folder",
    "directory containing thrift source files"
  )

  val scroogeThriftSources = SettingKey[Seq[File]](
    "scrooge-thrift-sources",
    "thrift source files to compile"
  )

  val scroogeThriftOutputFolder = SettingKey[File](
    "scrooge-thrift-output-folder",
    "output folder for generated scala files (defaults to sourceManaged)"
  )

  val scroogeIsDirty = TaskKey[Boolean](
    "scrooge-is-dirty",
    "true if scrooge has decided it needs to regenerate the scala files from thrift sources"
  )

  val scroogeGen = TaskKey[Seq[File]](
    "scrooge-gen",
    "generate scala code from thrift files using scrooge"
  )

  /**
   * these settings will go into both the compile and test configurations.
   * you can add them to other configurations by using inConfig(<config>)(genThriftSettings),
   * e.g. inConfig(Assembly)(genThriftSettings)
   */
  val genThriftSettings: Seq[Setting[_]] = Seq(
    scroogeThriftSourceFolder <<= (sourceDirectory) { _ / "thrift" },
    scroogeThriftSources <<= (scroogeThriftSourceFolder) { srcDir => (srcDir ** "*.thrift").get },
    scroogeThriftOutputFolder <<= (sourceManaged) { _ / "scala" },
    scroogeThriftIncludeFolders := Seq(),
    scroogeThriftNamespaceMap := Map(),

    // look at includes and our sources to see if anything is newer than any of our output files
    scroogeIsDirty <<= (
      streams,
      scroogeThriftSources,
      scroogeThriftOutputFolder,
      scroogeThriftIncludeFolders
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

    // actually run scrooge
    scroogeGen <<= (
      streams,
      scroogeIsDirty,
      scroogeThriftSources,
      scroogeThriftOutputFolder,
      scroogeFetch,
      scroogeBuildOptions,
      scroogeThriftIncludeFolders,
      scroogeThriftNamespaceMap
    ) map { (out, isDirty, sources, outputDir, jar, opts, inc, ns) =>
      // for some reason, sbt sometimes calls us multiple times, often with no source files.
      outputDir.mkdirs()
      if (isDirty && !sources.isEmpty) {
        out.log.info("Generating scrooge thrift for %s ...".format(sources.mkString(", ")))
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
    sourceGenerators <+= scroogeGen
  )

  val newSettings = Seq(
    scroogeVersion := "2.5.4",
    scroogeBuildOptions := Seq("--finagle"),
    scroogeName <<= (scroogeVersion) { ver => "scrooge-%s".format(ver) },
    scroogeCacheFolder <<= (baseDirectory) { (base) =>
      base / "project" / "target"
    },
    scroogeJar <<= (scroogeCacheFolder, scroogeName) { (folder, name) =>
      folder / (name + ".jar")
    },
    
    libraryDependencies <+= scalaVersion {
      // TODO: until we publish cross builds for real
      case x if x.startsWith("2.10") =>
        "com.twitter" %% "scrooge-runtime" % "2.4.0" intransitive()
      case _ =>
        "com.twitter" % "scrooge-runtime" % "2.3.2" intransitive()
    },

    scroogeFetch <<= (
      streams,
      scroogeCacheFolder,
      scroogeJar,
      scroogeVersion
    ) map { (out, cacheFolder, jar, ver) =>
      if (!jar.exists) {
        out.log.info("Fetching scrooge " + ver + " ...")

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

