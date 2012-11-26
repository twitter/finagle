package com.twitter.finagle.spdy

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._

import org.jboss.netty.handler.codec.http._
import org.jboss.netty.handler.codec.spdy.SpdyHttpHeaders

import com.twitter.finagle.{CancelledRequestException, WriteException}
import com.twitter.finagle.dispatch.ClientDispatcher
import com.twitter.finagle.transport.Transport

import com.twitter.util.{Future, Promise, Return, Throw}

class SpdyClientDispatcher(trans: Transport[HttpRequest, HttpResponse])
  extends ClientDispatcher[HttpRequest, HttpResponse]
{
  private[this] val readFailure = new AtomicReference[Throwable]()

  private[this] val promiseMap = new ConcurrentHashMap[java.lang.Integer, Promise[HttpResponse]]()

  private[this] def readLoop(): Future[_] = {
    trans.read() flatMap { resp =>
      val streamId = SpdyHttpHeaders.getStreamId(resp)
      val promise = promiseMap.remove(streamId)
      if (promise != null) {
        promise.updateIfEmpty(Return(resp))
      }
      readLoop()
    }
  }

  readLoop() onFailure { cause =>
    readFailure.synchronized {
      readFailure.set(cause)
      for (promise <- promiseMap.asScala.values) {
        promise.updateIfEmpty(Throw(cause))
      }
      promiseMap.clear()
    }
  }

  def apply(req: HttpRequest): Future[HttpResponse] = {
    val p = new Promise[HttpResponse]
    val streamId = SpdyHttpHeaders.getStreamId(req)

    readFailure.synchronized {
      val cause = readFailure.get()
      if (cause != null) {
        p() = Throw(WriteException(cause))
      } else {
        promiseMap.put(streamId, p)

        p.setInterruptHandler { case cause =>
          promiseMap.remove(streamId)
          p.updateIfEmpty(Throw(cause))
        }

        trans.write(req) onFailure { cause =>
          promiseMap.remove(streamId)
          p.updateIfEmpty(Throw(WriteException(cause)))
        }
      }
    }

    p
  }
}
