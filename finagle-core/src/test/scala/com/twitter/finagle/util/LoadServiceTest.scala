package com.twitter.finagle.util

import com.twitter.finagle.Announcer
import java.io.File
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.annotation.tailrec
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

}
