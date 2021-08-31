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

  test("offload early is on by default in clients") {
    offloadAtTheTop(StackClient.newStack[Unit, Unit])
  }

  test("offload early is on by default in servers") {
    offloadAtTheBottom(StackServer.newStack[Unit, Unit])
  }
}
