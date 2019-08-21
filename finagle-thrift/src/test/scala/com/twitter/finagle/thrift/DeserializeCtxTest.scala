package com.twitter.finagle.thrift

import com.twitter.finagle.service.ReqRep
import com.twitter.scrooge.ThriftStruct
import com.twitter.util.Return
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.FunSuite
import org.scalatestplus.mockito.MockitoSugar

class DeserializeCtxTest extends FunSuite with MockitoSugar {

  test("ClientDeserializeCtx only deserializes once") {
    val times = new AtomicInteger()
    val theVal = Return("hi")

    val deserializer = { bytes: Array[Byte] =>
      times.incrementAndGet()
      theVal
    }

    val deserCtx = new ClientDeserializeCtx(mock[ThriftStruct], deserializer)

    assert(theVal == deserCtx.deserialize(Array.empty))
    assert(1 == times.get)

    assert(theVal == deserCtx.deserialize(Array.empty))
    assert(1 == times.get)
  }

  test("ClientDeserializeCtx deserialize ignores input after first deserialize") {
    val deserializer = { bytes: Array[Byte] =>
      Return(bytes.length)
    }
    val deserCtx = new ClientDeserializeCtx(mock[ThriftStruct], deserializer)

    assert(Return(0) == deserCtx.deserialize(Array.empty))
    assert(Return(0) == deserCtx.deserialize(Array(9.toByte)))
  }

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
