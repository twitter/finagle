package com.twitter.finagle.util

import org.junit.runner.RunWith
import org.mockito.Mockito.{never, verify, when}
import org.mockito.Matchers.any
import org.scalatest.FunSuite
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import org.scalatest.mock.MockitoSugar
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class ExitGuardTest extends FunSuite with MockitoSugar with AssertionsForJUnit {
  test("guard creates thread, unguard kills thread") {
    val guard = ExitGuard.guard("test")
    val threads = Thread.getAllStackTraces.keySet.asScala.find { thread =>
      thread.getName.contains("Finagle ExitGuard")
    }

    assert(threads.nonEmpty)
    val thread = threads.get
    assert(!thread.isDaemon)
    assert(thread.isAlive)
    guard.unguard()
    assert(thread.isInterrupted, ExitGuard.explainGuards())
  }

  test("explain shows reason") {
    val guard = ExitGuard.guard("<%= reason %>")
    assert(ExitGuard.explainGuards().contains("<%= reason %>"))
    guard.unguard()
    assert(!ExitGuard.explainGuards().contains("<%= reason %>"))
  }
}
