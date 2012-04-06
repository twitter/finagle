package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.memcached.protocol.{
  ClientError,
  Error => MemcacheError,
  NonexistentCommand,
  ServerError
}
import org.jboss.netty.util.CharsetUtil
import org.specs.SpecificationWithJUnit

class ShowSpec extends SpecificationWithJUnit {
  "ResponseToEncoding" should {
    val responseToEncoding = new ResponseToEncoding

    "encode errors" >> {
      "ERROR" >> {
        val error = MemcacheError(new NonexistentCommand("No such command"))
        val res = responseToEncoding.encode(null, null, error)
        res must haveClass[Tokens]
        val tokens = res.asInstanceOf[Tokens]
        tokens.tokens must haveSize(1)
        tokens.tokens.head.toString(CharsetUtil.UTF_8) mustEqual "ERROR"
      }
      "CLIENT_ERROR" >> {
        val error = MemcacheError(new ClientError("Invalid Input"))
        val res = responseToEncoding.encode(null, null, error)
        res must haveClass[Tokens]
        val tokens = res.asInstanceOf[Tokens]
        tokens.tokens must haveSize(2)
        tokens.tokens.head.toString(CharsetUtil.UTF_8) mustEqual "CLIENT_ERROR"
      }
      "SERVER_ERROR" >> {
        val error = MemcacheError(new ServerError("Out of Memory"))
        val res = responseToEncoding.encode(null, null, error)
        res must haveClass[Tokens]
        val tokens = res.asInstanceOf[Tokens]
        tokens.tokens must haveSize(2)
        tokens.tokens.head.toString(CharsetUtil.UTF_8) mustEqual "SERVER_ERROR"
      }
    }
  }
}
