package com.twitter.finagle.http

import com.twitter.finagle
import com.twitter.finagle.Http.HttpImpl
import com.twitter.finagle.http.StreamingTest.Modifier
import com.twitter.finagle.http2.param.PriorKnowledge
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, Closable, Promise}
import java.util.concurrent.atomic.AtomicBoolean

class Http2StreamingTest extends AbstractStreamingTest {
  def impl: HttpImpl = finagle.Http.Http2[HttpImpl]
  override def configureClient: finagle.Http.Client => finagle.Http.Client = { client =>
    client.configured(PriorKnowledge(true))
  }
  override def configureServer: finagle.Http.Server => finagle.Http.Server = { server =>
    server.configured(PriorKnowledge(true))
  }

  // these must be changed slightly because an implementation detail of the
  // cleartext listener is that it must create an http/1.1 ChannelTransport
  // first, which is never used because we upgrade immediately.
  override def closingTransport(closed: Future[Unit]): (Modifier, Closable) = {
    val firstP = Promise[Unit]()
    @volatile var first = true
    (
      (transport: Transport[Any, Any]) => {
        if (first) {
          first = false
          firstP.ensure {
            transport.close()
          }
        } else {
          closed.ensure {
            transport.close()
          }
        }
        transport
      },
      Closable.make { _ =>
        firstP.setDone()
        Future.Done
      }
    )
  }

  override def closingOnceTransport(closed: Future[Unit]): (Modifier, Closable) = {
    val setFail = new AtomicBoolean(false)
    val firstP = Promise[Unit]()
    @volatile var first = true
    (
      (transport: Transport[Any, Any]) => {
        if (first) {
          first = false
          firstP.ensure {
            transport.close()
          }
        } else {
          if (!setFail.getAndSet(true)) closed.ensure {
            transport.close()
          }
        }
        transport
      },
      Closable.make { _ =>
        firstP.setDone()
        Future.Done
      }
    )
  }
}
