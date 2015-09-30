package com.twitter.finagle.util

import com.google.common.io.ByteStreams
import com.twitter.finagle.{Announcement, Announcer, Resolver}
import com.twitter.util.Future
import com.twitter.util.registry.{GlobalRegistry, SimpleRegistry, Entry}
import java.io.{File, InputStream}
import java.net.{InetSocketAddress, URL}
import java.util
import java.util.concurrent
import java.util.concurrent.{Callable, Executors, ExecutorService}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.collection.mutable
import scala.util.Random

// These traits correspond to files in:
// finagle-core/src/test/resources/META-INF/services

trait LoadServiceRandomInterface

class LoadServiceRandomInterfaceImpl extends LoadServiceRandomInterface

trait LoadServiceMaybeInterface

class LoadServiceFailingClass extends LoadServiceMaybeInterface {
  throw new RuntimeException("Cannot instanciate this!")
}

class LoadServiceGoodClass extends LoadServiceMaybeInterface

trait LoadServiceMultipleImpls
class LoadServiceMultipleImpls1 extends LoadServiceMultipleImpls
class LoadServiceMultipleImpls2 extends LoadServiceMultipleImpls
class LoadServiceMultipleImpls3 extends LoadServiceMultipleImpls


@RunWith(classOf[JUnitRunner])
class LoadServiceTest extends FunSuite with MockitoSugar {
  test("LoadService should apply[T] and return a set of instances of T") {
    assert(LoadService[Announcer]().nonEmpty)
  }

  test("LoadService should apply[T] and generate the right registry entries") {
    val simple = new SimpleRegistry
    GlobalRegistry.withRegistry(simple) {
      assert(LoadService[Resolver]().nonEmpty)
      assert(GlobalRegistry.get.toSet == Set(
        Entry(Seq("loadservice", "com.twitter.finagle.Resolver"), "com.twitter.finagle.TestResolver,com.twitter.finagle.TestAsyncInetResolver")
      ))
    }
  }

  test("LoadService should only load 1 instance of T, even when there's multiple occurence of T") {
    val randomIfaces = LoadService[LoadServiceRandomInterface]()
    assert(randomIfaces.size == 1)
  }

  test("LoadService should recover when seeing an initialization exception") {
    val randomIfaces = LoadService[LoadServiceMaybeInterface]()
    assert(randomIfaces.size == 1)
  }

  test("LoadService should only register successfully initialized classes") {
    val simple = new SimpleRegistry
    GlobalRegistry.withRegistry(simple) {
      val randomIfaces = LoadService[LoadServiceMaybeInterface]()
      assert(GlobalRegistry.get.toSet == Set(
        Entry(Seq("loadservice", "com.twitter.finagle.util.LoadServiceMaybeInterface"), "com.twitter.finagle.util.LoadServiceGoodClass")
      ))
    }
  }

  test("LoadService shouldn't fail on un-readable dir") {
    val loader = mock[ClassLoader]
    val buf = mutable.Buffer.empty[ClassPath.Info]
    val rand = new Random()

    val f = File.createTempFile("tmp", "__finagle_loadservice" + rand.nextInt(10000))
    f.delete
    if (f.mkdir()) {
      f.setReadable(false)
      ClassPath.browseUri(f.toURI, loader, buf)
      assert(buf.isEmpty)
      f.delete()
    }
  }

  test("LoadService shouldn't fail on un-readable sub-dir") {
    val loader = mock[ClassLoader]
    val buf = mutable.Buffer.empty[ClassPath.Info]
    val rand = new Random()

    val f = File.createTempFile("tmp", "__finagle_loadservice" + rand.nextInt(10000))
    f.delete
    if (f.mkdir()) {
      val subDir = new File(f.getAbsolutePath, "subdir")
      subDir.mkdir()
      assert(subDir.exists())
      subDir.setReadable(false)

      ClassPath.browseUri(f.toURI, loader, buf)
      assert(buf.isEmpty)

      subDir.delete()
      f.delete()
    }
  }

  test("LoadService should find services on non URLClassloader") {
    val loader = new MetaInfCodedClassloader(getClass.getClassLoader)
    // Run LoadService in a different thread from the custom classloader
    val clazz: Class[_] = loader.loadClass("com.twitter.finagle.util.LoadServiceCallable")
    val executor: ExecutorService = Executors.newSingleThreadExecutor()
    val future: concurrent.Future[Seq[Any]] = executor.submit(clazz.newInstance().asInstanceOf[Callable[Seq[Any]]])

    // Get the result
    val announcers: Seq[Any] = future.get()
    assert(announcers.exists(_.getClass.getName.endsWith("FooAnnouncer")),
      "Non-URLClassloader found announcer was not discovered"
    )
    executor.shutdown()
  }

  test("LoadService shouldn't fail on self-referencing jar") {
    import java.io._
    import java.util.jar._
    import Attributes.Name._
    val jarFile = File.createTempFile("test", ".jar")
    try {
      val manifest = new Manifest
      val attributes = manifest.getMainAttributes
      attributes.put(MANIFEST_VERSION, "1.0")
      attributes.put(CLASS_PATH, jarFile.getName)
      val jos = new JarOutputStream(new FileOutputStream(jarFile), manifest)
      jos.close
      val loader = mock[ClassLoader]
      val buf = mutable.Buffer.empty[ClassPath.Info]
      ClassPath.browseUri(jarFile.toURI, loader, buf)
    } finally {
      jarFile.delete
    }
  }

  test("LoadService shouldn't fail on circular referencing jar") {
    import java.io._
    import java.util.jar._
    import Attributes.Name._
    val jar1 = File.createTempFile("test", ".jar")
    val jar2 = File.createTempFile("test", ".jar")
    try {
      val manifest = new Manifest
      val attributes = manifest.getMainAttributes
      attributes.put(MANIFEST_VERSION, "1.0")
      attributes.put(CLASS_PATH, jar2.getName)
      new JarOutputStream(new FileOutputStream(jar1), manifest).close
      attributes.put(CLASS_PATH, jar1.getName)
      new JarOutputStream(new FileOutputStream(jar2), manifest).close
      val loader = mock[ClassLoader]
      val buf = mutable.Buffer.empty[ClassPath.Info]
      ClassPath.browseUri(jar1.toURI, loader, buf)
    } finally {
      jar1.delete
      jar2.delete
    }
  }

  test("LoadService should ignore packages according to ignoredPaths GlobalFlag") {
    loadServiceIgnoredPaths.let(Seq("foo/", "/bar")) {
      assert(ClassPath.ignoredPackages.takeRight(2) == Seq("foo/", "/bar"))
    }
  }

  test("LoadService should respect Denied if provided") {
    val denied1and2 = Set(
      "com.twitter.finagle.util.LoadServiceMultipleImpls1",
      "com.twitter.finagle.util.LoadServiceMultipleImpls2"
    )
    loadServiceDenied.let(denied1and2) {
      val loaded = LoadService[LoadServiceMultipleImpls]()
      assert(1 == loaded.size)
      assert(classOf[LoadServiceMultipleImpls3] == loaded.head.getClass)
    }
  }

}

class LoadServiceCallable extends Callable[Seq[Any]] {
  override def call(): Seq[Any] = LoadService[Announcer]()
}

class MetaInfCodedClassloader(parent: ClassLoader) extends ClassLoader(parent) {
  override def loadClass(name: String): Class[_] = {
    if (name.startsWith("com.twitter.finagle" )) {
      try {
        val path = name.replaceAll("\\.", "/") + ".class"
        val is: InputStream = getClass.getClassLoader.getResourceAsStream(path)
        val buf = ByteStreams.toByteArray(is)

        defineClass(name, buf, 0, buf.length)
      } catch {
        case e: Exception => throw new ClassNotFoundException("Couldn't load class " + name, e)
      }
    } else {
      parent.loadClass(name)
    }
  }

  override def getResources(p1: String): util.Enumeration[URL] = {
    // Totally contrived example classloader that stores "META-INF" as "HIDDEN-INF"
    // Not a good example of real-world issues, but it does the job at hiding the service definition from the
    // com.twitter.finagle.util.ClassPath code
    val resources: util.Enumeration[URL] = super.getResources(p1.replace("META-INF", "HIDDEN-INF"))
    if (resources == null) {
      super.getResources(p1)
    } else {
      resources
    }
  }
}

class FooAnnouncer extends Announcer {
  override val scheme: String = "foo"

  override def announce(addr: InetSocketAddress, name: String): Future[Announcement] = null
}

