package com.twitter.finagle

import com.twitter.finagle.client.StackClient
import com.twitter.finagle.filter.OffloadFilter
import com.twitter.finagle.server.StackServer
import org.scalatest.funsuite.AnyFunSuite

class OffloadEarlyTest extends AnyFunSuite {

  private def offloadAtTheTop(newStack: => Stack[ServiceFactory[Unit, Unit]]): Unit = {
    val total = newStack.tails.size
    val offloadAt = newStack.dropWhile(_.head.role != OffloadFilter.Role).tails.size
    assert(total - offloadAt > offloadAt)
  }

  private def offloadAtTheBottom(newStack: => Stack[ServiceFactory[Unit, Unit]]): Unit = {
    val total = newStack.tails.size
    val offloadAt = newStack.dropWhile(_.head.role != OffloadFilter.Role).tails.size
    assert(total - offloadAt < offloadAt)
  }

  test("offload early is off by default in clients") {
    offloadAtTheBottom(StackClient.newStack[Unit, Unit])
  }

  test("offload early is off by default in servers") {
    offloadAtTheTop(StackServer.newStack[Unit, Unit])
  }

  test("a toggle moves offload filter earlier in clients") {
    com.twitter.finagle.toggle.flag.overrides.let("com.twitter.finagle.OffloadEarly", 1.0) {
      offloadAtTheTop(StackClient.newStack[Unit, Unit])
    }
  }

  test("a toggle moves offload filter earlier in servers") {
    com.twitter.finagle.toggle.flag.overrides.let("com.twitter.finagle.OffloadEarly", 1.0) {
      offloadAtTheBottom(StackServer.newStack[Unit, Unit])
    }
  }
}
