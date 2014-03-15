package com.twitter.finagle

import com.twitter.finagle.client._
import com.twitter.finagle.netty3._
import com.twitter.finagle.pool.ReusingPool
import com.twitter.finagle.server._
import com.twitter.finagle.stats.{ClientStatsReceiver, StatsReceiver}
import com.twitter.util.{CloseAwaitably, Future, Promise, Return, Time}
import java.net.SocketAddress
import org.jboss.netty.buffer.ChannelBuffer

object MuxTransporter extends Netty3Transporter[ChannelBuffer, ChannelBuffer](
  "mux", mux.PipelineFactory)

object MuxClient extends DefaultClient[ChannelBuffer, ChannelBuffer](
  name = "mux",
  endpointer = (sa, sr) => (Bridge[ChannelBuffer, ChannelBuffer, ChannelBuffer, ChannelBuffer](
    MuxTransporter, new mux.ClientDispatcher(_, sr))(sa, sr)),
  pool = (sr: StatsReceiver) => new ReusingPool(_, sr.scope("reusingpool")))

object MuxListener extends Netty3Listener[ChannelBuffer, ChannelBuffer]("mux", mux.PipelineFactory)
object MuxServer extends DefaultServer[ChannelBuffer, ChannelBuffer, ChannelBuffer, ChannelBuffer](
  "mux", MuxListener, new mux.ServerDispatcher(_, _, true)
)

/**
 * A client and server for the mux protocol described in [[com.twitter.finagle.mux]].
 */
object Mux extends Client[ChannelBuffer, ChannelBuffer] with Server[ChannelBuffer, ChannelBuffer] {
  def newClient(dest: Name, label: String): ServiceFactory[ChannelBuffer, ChannelBuffer] =
    MuxClient.newClient(dest, label)

  def serve(addr: SocketAddress, service: ServiceFactory[ChannelBuffer, ChannelBuffer]): ListeningServer =
    MuxServer.serve(addr, service)
}

package exp {

  object MuxSession {
    /**
     * Connect to addr to establish a mux session. The returned function
     * should be applied to a "receiver" Session and will return a
     * "sender" Session. The "receiver" Session is used to handle
     * incoming RPCs, messages, and pings from addr. The
     * "sender" Session can be used to send RPCs, messages, and
     * pings to the addr.
     */
    def connect(addr: SocketAddress): Future[mux.NewSession] =
      MuxTransporter(addr, ClientStatsReceiver) map { transport =>
        { receiver: mux.Session =>
          val sender = new mux.SenderSession(transport)
          val dispatcher = new mux.SessionDispatcher(transport, sender, receiver)
          sender
        }
      }

    /**
     * Listen for connections on addr. For each connection, newSession
     * is invoked with a "sender" instance of Session and is
     * expected to return a "receiver" instance of Session. The
     * "sender" Session can be used to send RPCs, messages, and
     * pings to the connector.  The "receiver" Session is used to handle
     * incoming RPCs, messages, and pings from the connector.
     */
    def listen(
      addr: SocketAddress,
      newSession: mux.NewSession
    ) = new ListeningServer with CloseAwaitably {
      private[this] val closeDeadline = new Promise[Time]

      val listener = MuxListener.listen(addr) { transport =>
        val sender = new mux.SenderSession(transport)
        val receiver = newSession(sender)
        val dispatcher = new mux.SessionDispatcher(transport, sender, receiver)
        closeDeadline onSuccess { deadline =>
          sender.drain() respond { _ =>
            dispatcher.close(deadline)
          }
        }
      }

      def closeServer(deadline: Time) = closeAwaitably {
        closeDeadline.updateIfEmpty(Return(deadline))
        listener.close(deadline)
      }

      def boundAddress = listener.boundAddress
    }
  }

  private[finagle]
  object MuxNetty3Stack extends Netty3Stack[Any, Any, ChannelBuffer, ChannelBuffer](
    "mux",
    mux.PipelineFactory,
    (transport, statsReceiver) =>
      new mux.ClientDispatcher(transport.cast[ChannelBuffer, ChannelBuffer], statsReceiver)
  )

  private[finagle]
  object ReusingPoolModule
    extends Stack.Simple[ServiceFactory[ChannelBuffer, ChannelBuffer]](StackClient.Role.ReusingPool)
  {
    def make(params: Stack.Params, nextFac: ServiceFactory[ChannelBuffer, ChannelBuffer]) = {
      val StackClient.Stats(statsReceiver, _) = params[StackClient.Stats]
      new ReusingPool(nextFac, statsReceiver.scope("reusingpool"))
    }
  }

  private[finagle]
  class MuxClient(client: StackClient[ChannelBuffer, ChannelBuffer])
    extends RichStackClient[ChannelBuffer, ChannelBuffer, MuxClient](client)
  {
    protected def newRichClient(client: StackClient[ChannelBuffer, ChannelBuffer]) =
      new MuxClient(client)
  }

  object MuxClient extends MuxClient(new StackClient({
    val reusingClientStack = StackClient.clientStack[ChannelBuffer, ChannelBuffer]
      .replace(StackClient.Role.Pool, ReusingPoolModule)

    reusingClientStack ++ (MuxNetty3Stack +: StackClient.nilStack)
  }))
}
