package com.twitter.finagle.mux.stats

import com.twitter.finagle.mux.ClientDiscardedRequestException
import org.scalatest.funsuite.AnyFunSuite

class MuxCancelledCategorizerTest extends AnyFunSuite {
  test("MuxCancelledCategorizer knows when things get cancelled") {
    val categorizer = MuxCancelledCategorizer.Instance.lift

    assert(categorizer(new ClientDiscardedRequestException("")) == Some("cancelled"))
    assert(categorizer(new RuntimeException("")) == None)

    assert(categorizer(new Exception(new ClientDiscardedRequestException(""))) == Some("cancelled"))
    assert(
      categorizer(new Exception(new Exception(new ClientDiscardedRequestException("")))) == Some(
        "cancelled"
      )
    )

    assert(categorizer(new Exception(new Exception(new RuntimeException("")))) == None)
  }
}
