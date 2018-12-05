package com.twitter.finagle.netty4.pushsession

import com.twitter.finagle.Stack.Params
import com.twitter.finagle.{ChannelClosedException, ListeningServer, Service, Status}
import com.twitter.finagle.pushsession.{PushChannelHandle, PushSession}
import com.twitter.finagle.netty4.AbstractNetty4ListenerTest
import com.twitter.util._
import io.netty.channel.ChannelPipeline
import java.net.SocketAddress

class Netty4PushListenerTest extends AbstractNetty4ListenerTest {

  // Contrived server session that expects only one request at a time. This is
  // for the benefit of the "notices when the client cuts the connection" test.
  private class SimpleSession[Req, Rep](
    handle: PushChannelHandle[Req, Rep],
    service: Service[Req, Rep])
      extends PushSession[Req, Rep](handle) {

    @volatile
    private[this] var currentDispatch: Future[Rep] = Future.never

    handle.onClose.respond { r =>
      val exc = r match {
        case Throw(t) => t
        case Return(_) => new ChannelClosedException()
      }
      currentDispatch.raise(exc)
    }

    def receive(message: Req): Unit = {
      currentDispatch = service(message)
      currentDispatch.respond {
        case Return(rep) => handle.sendAndForget(rep)
        case Throw(_) => handle.close()
      }
    }

    def status: Status = handle.status

    def close(deadline: Time): Future[Unit] = handle.close(deadline)
  }

  protected def serveService[Req: Manifest, Rep: Manifest](
    address: SocketAddress,
    init: (ChannelPipeline) => Unit,
    params: Params,
    service: Service[Req, Rep]
  ): ListeningServer = {
    val listener = new Netty4PushListener[Req, Rep](init, params, identity)
    listener.listen(address) { handle =>
      Future.value(new SimpleSession[Req, Rep](handle, service))
    }
  }
}
