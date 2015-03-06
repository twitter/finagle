package com.twitter.finagle.util

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import org.scalatest.mock.MockitoSugar
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class ExitGuardTest
  extends FunSuite
  with MockitoSugar
  with AssertionsForJUnit
  with Eventually
  with IntegrationPatience {

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
    eventually { assert(!thread.isAlive, ExitGuard.explainGuards()) }
  }

  test("explain shows reason") {
    val guard = ExitGuard.guard("<%= reason %>")
    assert(ExitGuard.explainGuards().contains("<%= reason %>"))
    guard.unguard()
    assert(!ExitGuard.explainGuards().contains("<%= reason %>"))
  }
}
