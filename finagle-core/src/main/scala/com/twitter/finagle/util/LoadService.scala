package com.twitter.finagle.util

import com.twitter.app.GlobalFlag
import com.twitter.logging.Level
import com.twitter.util.NonFatal
import com.twitter.util.registry.GlobalRegistry
import java.io.{File, IOException}
import java.net.{URI, URISyntaxException, URLClassLoader}
import java.nio.charset.MalformedInputException
import java.util.ServiceConfigurationError
import java.util.jar.JarFile
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.reflect.ClassTag

/**
 * Inspect and load the classpath. Inspired by Guava's ClassPath
 * utility.
 *
 * @note This is not a generic facility, rather it is designed
 * specifically to support LoadService.
 */
private object ClassPath {

  private val defaultIgnoredPackages = Seq(
    "apple/", "ch/epfl/", "com/apple/", "com/oracle/",
    "com/sun/", "java/", "javax/", "scala/", "sun/", "sunw/")

  private[util] def ignoredPackages = defaultIgnoredPackages ++ loadServiceIgnoredPaths()

  /**
   * Information about a classpath entry.
   */
  case class Info(path: String, iface: String, lines: Seq[String])

  /**
   * Browse the given classloader recursively from
   * its package root.
   */
  def browse(loader: ClassLoader): Seq[Info] = {
    val buf = mutable.Buffer[Info]()

    for ((uri, loader) <- getEntries(loader))
      browseUri(uri, loader, buf)

    buf
  }

  private def ifaceOfName(name: String) =
    if (!(name contains "META-INF")) None
    else name.split("/").takeRight(3) match {
      case Array("META-INF", "services", iface) => Some(iface)
      case _ => None
    }

  private def getEntries(loader: ClassLoader): Seq[(URI, ClassLoader)] = {
    val ents = mutable.Buffer[(URI, ClassLoader)]()
    val parent = loader.getParent
    if (parent != null)
      ents ++= getEntries(parent)

    loader match {
      case urlLoader: URLClassLoader =>
        for (url <- urlLoader.getURLs) {
          ents += (url.toURI -> loader)
        }
      case _ =>
    }

    ents
  }

  private[finagle] def browseUri(
    uri: URI,
    loader: ClassLoader,
    buf: mutable.Buffer[Info]
  ): Unit = browseUri0(uri, loader, buf, mutable.Set[String]())

  private def browseUri0(
    uri: URI,
    loader: ClassLoader,
    buf: mutable.Buffer[Info],
    history: mutable.Set[String]
  ): Unit = {

    if (uri.getScheme != "file")
      return
    val f = new File(uri)
    if (!(f.exists() && f.canRead))
      return
    val path = f.getCanonicalPath
    if (history.contains(path))
      return

    history.add(path)

    if (f.isDirectory)
      browseDir(f, loader, "", buf)
    else
      browseJar(f, loader, buf, history)
  }

  private def browseDir(
      dir: File, loader: ClassLoader,
      prefix: String, buf: mutable.Buffer[Info]) {
    if (ignoredPackages contains prefix)
      return

    for (f <- dir.listFiles)
      if (f.isDirectory && f.canRead)
        browseDir(f, loader, prefix + f.getName + "/", buf)
      else for (iface <- ifaceOfName(prefix + f.getName)) {
        val source = Source.fromFile(f, "UTF-8")
        val lines = readLines(source)
        buf += Info(prefix + f.getName, iface, lines)
      }
  }

  private def browseJar(file: File, loader: ClassLoader, buf: mutable.Buffer[Info], history: mutable.Set[String]) {
    val jarFile = try new JarFile(file) catch {
      case _: IOException => return  // not a Jar file
    }

    try {
      for (uri <- jarClasspath(file, jarFile.getManifest))
        browseUri0(uri, loader, buf, history)

      for {
        e <- jarFile.entries.asScala
        if !e.isDirectory
        n = e.getName
        if !(ignoredPackages exists (n startsWith _))
        iface <- ifaceOfName(n)
      } {
        val source = Source.fromInputStream(jarFile.getInputStream(e), "UTF-8")
        val lines = readLines(source)
        buf += Info(n, iface, lines)
      }
    } finally {
      try jarFile.close() catch {
        case _: IOException =>
      }
    }
  }

  private def jarClasspath(jarFile: File, manifest: java.util.jar.Manifest): Seq[URI] = for {
    m <- Option(manifest).toSeq
    attr <- Option(m.getMainAttributes.getValue("Class-Path")).toSeq
    el <- attr.split(" ")
    uri <- uriFromJarClasspath(jarFile, el)
  } yield uri

  private def uriFromJarClasspath(jarFile: File, path: String) = try {
    val uri = new URI(path)
    if (uri.isAbsolute)
      Some(uri)
    else
      Some(new File(jarFile.getParentFile, path.replace('/', File.separatorChar)).toURI)
  } catch {
    case _: URISyntaxException => None
  }

  private[util] def readLines(source: Source): Seq[String] = {
    try {
      source.getLines().toArray.flatMap { line =>
        val commentIdx = line.indexOf('#')
        val end = if (commentIdx != -1) commentIdx else line.length
        val str = line.substring(0, end).trim
        if (str.isEmpty) Nil else Seq(str)
      }
    } catch {
      case ex: MalformedInputException => Nil /* skip malformed files (e.g. non UTF-8) */
    } finally {
      source.close()
    }
  }
}

object loadServiceIgnoredPaths extends GlobalFlag[Seq[String]](
    Seq.empty[String],
    "Additional packages to be excluded from recursive directory scan")

/**
 * A deny list of implementations to ignore. Keys are the fully qualified class names.
 * Any other implementations that are found via `LoadService.apply` are eligible to be used.
 *
 * As an example, here's how to filter out `OstrichStatsReceiver`, `OstrichExporter` and 
 * `CommonsStatsReceiver` using a global flag:
 *
 * {{{
 * -Dcom.twitter.finagle.util.loadServiceDenied=com.twitter.finagle.stats.OstrichStatsReceiver,com.twitter.finagle.stats.OstrichExporter,com.twitter.finagle.stats.CommonsStatsReceiver
 * }}}
 *
 * We need to pass the arguments as Java property values instead of as Java application
 * arguments (regular TwitterServer flags) because [[LoadService]] may be loaded before
 * application arguments are parsed.
 */
object loadServiceDenied extends GlobalFlag[Set[String]](
    Set.empty,
    "A deny list of implementations to ignore. Keys are the fully qualified class names. " +
      "Any other implementations that are found via `LoadService.apply` are eligible to be used.")

/**
 * Load a singleton class in the manner of [[java.util.ServiceLoader]]. It is
 * more resilient to varying Java packaging configurations than ServiceLoader.
 */
object LoadService {

  private val cache: mutable.Map[ClassLoader, Seq[ClassPath.Info]] = mutable.Map.empty

  def apply[T: ClassTag](): Seq[T] = synchronized {
    val iface = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val ifaceName = iface.getName
    val loader = iface.getClassLoader

    val denied: Set[String] = loadServiceDenied()

    val classNames = for {
      info <- cache.getOrElseUpdate(loader, ClassPath.browse(loader))
      if info.iface == ifaceName
      className <- info.lines
    } yield className

    val classNamesFromResources = for {
      rsc <- loader.getResources("META-INF/services/" + ifaceName).asScala
      line <- ClassPath.readLines(Source.fromURL(rsc, "UTF-8"))
    } yield line

    val buffer = mutable.ListBuffer.empty[String]
    val result = (classNames ++ classNamesFromResources)
      .distinct
      .filterNot { className =>
        val isDenied = denied.contains(className)
        if (isDenied)
          DefaultLogger.info(s"LoadService: skipped $className due to deny list flag")
        isDenied
      }
      .flatMap { className =>
        val cls = Class.forName(className)
        if (!iface.isAssignableFrom(cls))
          throw new ServiceConfigurationError(s"$className not a subclass of $ifaceName")

        DefaultLogger.log(
          Level.DEBUG,
          s"LoadService: loaded instance of class $className for requested service $ifaceName"
        )

        try {
          val instance = cls.newInstance().asInstanceOf[T]
          buffer += className
          Some(instance)
        } catch {
          case NonFatal(ex) =>
            DefaultLogger.log(
              Level.FATAL,
              s"LoadService: failed to instantiate '$className' for the requested "
                + s"service '$ifaceName'",
              ex
            )
            None
        }
      }

    GlobalRegistry.get.put(Seq("loadservice", ifaceName), buffer.mkString(","))
    result
  }
}

