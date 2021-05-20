package com.twitter.finagle.thrift

import com.twitter.finagle.service.ReqRep
import com.twitter.util.Return
import org.scalatest.funsuite.AnyFunSuite

class ServerToReqRepTest extends AnyFunSuite {

  test("ServerToReqRep only set the ReqRep once") {
    val reqRep1 = ReqRep(1, Return(1))
    val reqRep2 = ReqRep(2, Return(2))
    val deserCtx = new ServerToReqRep
    deserCtx.setReqRep(reqRep1)
    assert(reqRep1 == deserCtx(Array.empty))

    deserCtx.setReqRep(reqRep2)
    assert(reqRep1 == deserCtx(Array.empty))
  }
}
