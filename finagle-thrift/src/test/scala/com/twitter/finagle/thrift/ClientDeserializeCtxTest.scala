package com.twitter.finagle.thrift

import com.twitter.scrooge.ThriftStruct
import com.twitter.util.Return
import java.util.concurrent.atomic.AtomicInteger
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ClientDeserializeCtxTest extends AnyFunSuite with MockitoSugar {

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
    val deserializer = { bytes: Array[Byte] => Return(bytes.length) }
    val deserCtx = new ClientDeserializeCtx(mock[ThriftStruct], deserializer)

    assert(Return(0) == deserCtx.deserialize(Array.empty))
    assert(Return(0) == deserCtx.deserialize(Array(9.toByte)))
  }

  test("deserialized can be called multiple times when batched") {
    val times = new AtomicInteger()

    val deserializer = { bytes: Array[Byte] =>
      times.incrementAndGet()
      Return(bytes.length)
    }

    val deserCtx = new ClientDeserializeCtx(mock[ThriftStruct], deserializer)

    val mergedResponse = (0 to 9).map { i =>
      deserCtx.deserializeFromBatched(Array.fill(i)(i.toByte))
    }.last

    deserCtx.mergedDeserializedResponse(mergedResponse)
    assert(deserCtx.deserialize(Array.empty) == Return(9))
    // the deserCtx.deserialize wont deserialize the response
    // total number is 10 instead of 11
    assert(times.get == 10)
  }
}
