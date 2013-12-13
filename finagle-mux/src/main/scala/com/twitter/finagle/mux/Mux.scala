package com.twitter.finagle

import com.twitter.finagle.client._
import com.twitter.finagle.netty3._
import com.twitter.finagle.pool.ReusingPool
import com.twitter.finagle.server._
import com.twitter.finagle.stats.ClientStatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.util.{CloseAwaitably, Future, Promise, Return, Time}
import java.net.SocketAddress
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.stats.StatsReceiver

object MuxTransporter extends Netty3Transporter[ChannelBuffer, ChannelBuffer](
  "mux", mux.PipelineFactory)

object MuxClient extends DefaultClient[ChannelBuffer, ChannelBuffer](
  name = "mux",
  endpointer = Bridge[ChannelBuffer, ChannelBuffer, ChannelBuffer, ChannelBuffer](
    MuxTransporter, new mux.ClientDispatcher(_)),
  pool = (sr: StatsReceiver) => new ReusingPool(_, sr.scope("reusingpool"))
)

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
}


