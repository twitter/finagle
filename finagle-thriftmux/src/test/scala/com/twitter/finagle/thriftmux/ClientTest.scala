package com.twitter.finagle.thriftmux

import com.twitter.finagle.ThriftMux
import com.twitter.finagle.filter.NackAdmissionFilter
import org.scalatest.funsuite.AnyFunSuite

class ClientTest extends AnyFunSuite {
  test("client stack includes exactly one NackAdmissionFilter") {
    val stack = ThriftMux.Client().stack

    assert(stack.tails.count(_.head.role == NackAdmissionFilter.role) == 1)
  }
}
