package com.twitter.finagle.thriftmux

import com.twitter.finagle.ThriftMux
import com.twitter.finagle.filter.NackAdmissionFilter
import org.scalatest.FunSuite

class ClientTest extends FunSuite {
  test("client stack includes NackAdmissionFilter") {
    val stack = ThriftMux.Client().stack
    assert(stack.contains(NackAdmissionFilter.role))
  }
}
