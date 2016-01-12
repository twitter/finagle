package com.twitter.finagle.thrift

import com.twitter.scrooge.ThriftStruct
import com.twitter.util.Return
import java.util.concurrent.atomic.AtomicInteger
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class DeserializeCtxTest extends FunSuite
  with MockitoSugar {

  test("only deserializes once") {
    val times = new AtomicInteger()
    val theVal = Return("hi")

    val deserializer = { bytes: Array[Byte] =>
      times.incrementAndGet()
      theVal
    }

    val deserCtx = new DeserializeCtx(mock[ThriftStruct], deserializer)

    assert(theVal == deserCtx.deserialize(Array.empty))
    assert(1 == times.get)

    assert(theVal == deserCtx.deserialize(Array.empty))
    assert(1 == times.get)
  }

  test("deserialize ignores input after first deserialize") {
    val deserializer = { bytes: Array[Byte] =>
      Return(bytes.length)
    }
    val deserCtx = new DeserializeCtx(mock[ThriftStruct], deserializer)

    assert(Return(0) == deserCtx.deserialize(Array.empty))
    assert(Return(0) == deserCtx.deserialize(Array(9.toByte)))
  }

}
