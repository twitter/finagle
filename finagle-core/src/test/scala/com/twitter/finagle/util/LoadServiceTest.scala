package com.twitter.finagle.util

import com.twitter.finagle.Announcer
import java.io.File
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.util.Random
import scala.collection.mutable

trait LoadServiceRandomInterface

class LoadServiceRandomInterfaceImpl extends LoadServiceRandomInterface

@RunWith(classOf[JUnitRunner])
class LoadServiceTest extends FunSuite with MockitoSugar {
  test("LoadService should apply[T] and return a set of instances of T") {
    assert(LoadService[Announcer]().nonEmpty)
  }

  test("LoadService should only load 1 instance of T, even when there's multiple occurence of T") {
    val randomIfaces = LoadService[LoadServiceRandomInterface]()
    assert(randomIfaces.size == 1)
  }

  test("LoadService shouldn't fail on un-readable dir") {
    val loader = mock[ClassLoader]
    val buf = mutable.Buffer[ClassPath.Info]()
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
    val buf = mutable.Buffer[ClassPath.Info]()
    val rand = new Random()

    val f = File.createTempFile("tmp", "__finagle_loadservice" + rand.nextInt(10000))
    f.delete
    if (f.mkdir()) {
      val subDir = new File(f.getAbsolutePath(), "subdir")
      subDir.mkdir()
      assert(subDir.exists())
      subDir.setReadable(false)

      ClassPath.browseUri(f.toURI, loader, buf)
      assert(buf.isEmpty)

      subDir.delete()
      f.delete()
    }
  }

}
