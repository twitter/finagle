package com.twitter.finagle.util

import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatestplus.junit.AssertionsForJUnit
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ExitGuardTest
    extends AnyFunSuite
    with MockitoSugar
    with AssertionsForJUnit
    with Eventually
    with IntegrationPatience {

  test("guard creates thread, unguard removes") {
    val name = s"ExitGuardTest-${System.nanoTime}"
    val guard = ExitGuard.guard(name)
    val (thread, guards) = ExitGuard.guards.get

    assert(!thread.isDaemon)
    assert(thread.isAlive)
    assert(guards.map(_.reason).contains(name))

    guard.unguard()

    // depending on what has been registered and unregistered,
    // either there should be no guards or our name should not be in the list.
    ExitGuard.guards match {
      case None =>
        eventually { assert(!thread.isAlive, ExitGuard.explainGuards()) }
      case Some((_, gs)) =>
        assert(!gs.map(_.reason).contains(name))
    }
  }

  test("explain shows reason") {
    val guard = ExitGuard.guard("<%= reason %>")
    assert(ExitGuard.explainGuards().contains("<%= reason %>"))
    guard.unguard()
    assert(!ExitGuard.explainGuards().contains("<%= reason %>"))
  }
}
