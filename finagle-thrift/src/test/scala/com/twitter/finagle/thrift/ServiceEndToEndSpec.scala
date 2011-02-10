package com.twitter.finagle.thrift

import org.specs.Specification

import collection.JavaConversions._

import org.apache.thrift.TBase

import org.jboss.netty.channel._
import org.jboss.netty.channel.local._

import com.twitter.util.RandomSocket
import com.twitter.finagle.Service
import com.twitter.finagle.builder._
import com.twitter.finagle.service._
import com.twitter.finagle.util.Conversions._

import com.twitter.silly.Silly
import com.twitter.util.{Future, Promise, Try}
import com.twitter.util.TimeConversions._

object ServiceEndToEndSpec extends Specification {
  type AnyCall = ThriftCall[_ <: TBase[_, _], _ <: TBase[_, _]]

  class SillyService extends Service[ThriftCall[_,_], ThriftReply[_]] {
    def apply(call: ThriftCall[_,_]) = Future {
      call match {
        case bleep: ThriftCall[Silly.bleep_args, Silly.bleep_result]
        if bleep.method.equals("bleep") =>
          val response = bleep.newReply
          response.setSuccess(bleep.arguments.request.reverse)
          bleep.reply(response)
        case _ =>
          throw new IllegalArgumentException("Invalid method!!")
      }
    }
  }

  "Service based Thrift server" should {
    ThriftTypes.add(
      new ThriftCallFactory[Silly.bleep_args, Silly.bleep_result]
      ("bleep", classOf[Silly.bleep_args], classOf[Silly.bleep_result]))

    val addr = RandomSocket.nextAddress()

    val sillyService = new SillyService()

    val server = ServerBuilder()
      .codec(new Thrift)
      .bindTo(addr)
      .build(sillyService)

    doAfter { server.close(20.milliseconds) }

    "with wrapped replies" in {
      "respond to calls with ThriftReply[Call.response_type]" in {
        val client = ClientBuilder()
          .codec(new Thrift)
          .hosts(Seq(addr))
          .build

        val promise = new Promise[ThriftReply[_]]

        val call = new ThriftCall("bleep",
                                  new Silly.bleep_args("hello"),
                                  classOf[Silly.bleep_result])
        client(call) respond { r => promise() = r }

        val result = promise.get(1.second)

        result.isReturn must beTrue
        val reply = result().asInstanceOf[ThriftReply[Silly.bleep_result]]
        reply().response.success mustEqual "olleh"
      }
    }
  }
}
