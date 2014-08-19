package com.twitter.finagle.util

import java.net.{InetSocketAddress, URL}
import java.util
import java.util.concurrent
import java.util.concurrent.{Callable, ExecutorService, Executors}

import com.twitter.finagle.{Announcement, Announcer}
import java.io.{InputStream, File}
import com.twitter.util.Future
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.util.Random
import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class LoadServiceTest extends FunSuite with MockitoSugar {
  test("LoadService should apply[T] and return a set of instances of T") {
    assert(LoadService[Announcer]().nonEmpty)
  }

  test("LoadService shouldn't fail on un-readable dir") {
    val loader = mock[ClassLoader]
    val buf = mutable.Buffer[ClassPath.Info]()
    val rand = new Random()

    val f = File.createTempFile("/tmp", "__finagle_loadservice" + rand.nextInt(10000))
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
    val buf = mutable.Buffer[ClassPath.Info]()
    val rand = new Random()

    val f = File.createTempFile("/tmp", "__finagle_loadservice" + rand.nextInt(10000))
    f.delete
    if (f.mkdir()) {
      val subDir = new File(f.getAbsolutePath() + "/subdir/")
      subDir.mkdir()
      assert(subDir.exists())
      subDir.setReadable(false)

      try {
        ClassPath.browseUri(f.toURI, loader, buf)
        assert(buf.isEmpty)
      } catch {
        case e: NullPointerException =>
          e.printStackTrace()
          fail()
      }

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
    assert(announcers.find(_.getClass.getName.endsWith("FooAnnouncer")).isDefined,
      "Non-URLClassloader found announcer was not discovered"
    )
  }

}

class LoadServiceCallable extends Callable[Seq[Any]] {
  override def call(): Seq[Any] = LoadService[Announcer]()
}

class MetaInfCodedClassloader(parent: ClassLoader) extends ClassLoader(parent) {
  override def loadClass(p1: String): Class[_] = {
    if (p1.startsWith("com.twitter.finagle" )) {
      try {
        val is: InputStream = getClass.getClassLoader().getResourceAsStream(p1.replaceAll("\\.", "/") + ".class")
        val buf = new Array[Byte](10000)
        val len = is.read(buf)
        defineClass(p1, buf, 0, len)
      } catch {
        case e => throw new ClassNotFoundException("Couldn't load class " + p1, e)
      }
    } else {
      parent.loadClass(p1)
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
