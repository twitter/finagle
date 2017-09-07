package com.twitter.finagle

import com.twitter.finagle.client.{StackClient, StdStackClient, Transporter}
import com.twitter.finagle.dispatch.{
  GenSerialClientDispatcher,
  SerialClientDispatcher,
  SerialServerDispatcher
}
import com.twitter.finagle.netty3.{Netty3Listener, Netty3Transporter}
import com.twitter.finagle.netty3.transport.ChannelTransport
import com.twitter.finagle.param.{Label, ProtocolLibrary, Stats}
import com.twitter.finagle.server.{Listener, StackServer, StdStackServer}
import com.twitter.finagle.service.FailFastFactory
import com.twitter.finagle.service.FailFastFactory.FailFast
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.TraceInitializerFilter
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.util.{Closable, Future, Time}
import java.net.{InetSocketAddress, SocketAddress}
import org.jboss.netty.channel.{Channel, ChannelPipeline, ChannelPipelineFactory}

/**
 * Codecs provide protocol encoding and decoding via netty pipelines
 * as well as a standard filter stack that is applied to services
 * from this codec.
 */
trait Codec[Req, Rep] {

  /**
   * The pipeline factory that implements the protocol.
   */
  def pipelineFactory: ChannelPipelineFactory

  /* Note: all of the below interfaces are scheduled for deprecation in favor of
   * clients/servers
   */

  /**
   * Prepare a factory for usage with the codec. Used to allow codec
   * modifications to the service at the top of the network stack.
   */
  def prepareServiceFactory(
    underlying: ServiceFactory[Req, Rep]
  ): ServiceFactory[Req, Rep] =
    underlying

  /**
   * Prepare a connection factory. Used to allow codec modifications
   * to the service at the bottom of the stack (connection level).
   */
  final def prepareConnFactory(underlying: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] =
    prepareConnFactory(underlying, Stack.Params.empty)

  def prepareConnFactory(
    underlying: ServiceFactory[Req, Rep],
    params: Stack.Params
  ): ServiceFactory[Req, Rep] = underlying

  /**
   * Note: the below ("raw") interfaces are low level, and require a
   * good understanding of finagle internals to implement correctly.
   * Proceed with care.
   */
  def newClientTransport(ch: Channel, statsReceiver: StatsReceiver): Transport[Any, Any] =
    new ChannelTransport(ch)

  final def newClientDispatcher(transport: Transport[Any, Any]): Service[Req, Rep] =
    newClientDispatcher(transport, Stack.Params.empty)

  def newClientDispatcher(
    transport: Transport[Any, Any],
    params: Stack.Params
  ): Service[Req, Rep] = {
    // In order to not break the Netty 3 API, we provide some 'alternative facts'
    // and continue without our dynamic check
    val clazz = classOf[Any].asInstanceOf[Class[Rep]]
    new SerialClientDispatcher(
      Transport.cast[Req, Rep](clazz, transport),
      params[param.Stats].statsReceiver.scope(GenSerialClientDispatcher.StatsScope)
    )
  }

  def newServerDispatcher(
    transport: Transport[Any, Any],
    service: Service[Req, Rep]
  ): Closable = {
    // In order to not break the Netty 3 API, we provide some 'alternative facts'
    // and continue without our dynamic check
    val clazz = classOf[Any].asInstanceOf[Class[Req]]
    new SerialServerDispatcher[Req, Rep](Transport.cast[Rep, Req](clazz, transport), service)
  }

  /**
   * Is this Codec OK for failfast? This is a temporary hack to
   * disable failFast for codecs for which it isn't well-behaved.
   */
  def failFastOk: Boolean = true

  /**
   * A hack to allow for overriding the TraceInitializerFilter when using
   * Client/Server Builders rather than stacks.
   */
  def newTraceInitializer: Stackable[ServiceFactory[Req, Rep]] =
    TraceInitializerFilter.clientModule[Req, Rep]

  /**
   * A protocol library name to use for displaying which protocol library this client or server is using.
   */
  def protocolLibraryName: String = "not-specified"

  /**
   * Converts this codec into a [[StackClient]].
   */
  def toStackClient: StackClient[Req, Rep] = CodecClient(_ => this)

  /**
   * Converts this codec into a [[StackServer]].
   */
  def toStackServer: StackServer[Req, Rep] = CodecServer(_ => this)
}

/**
 * An abstract class version of the above for java compatibility.
 */
abstract class AbstractCodec[Req, Rep] extends Codec[Req, Rep]

object Codec {
  def ofPipelineFactory[Req, Rep](makePipeline: => ChannelPipeline) =
    new Codec[Req, Rep] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = makePipeline
      }
    }

  def ofPipeline[Req, Rep](p: ChannelPipeline) = new Codec[Req, Rep] {
    def pipelineFactory = new ChannelPipelineFactory {
      def getPipeline = p
    }
  }
}

/**
 * Codec factories create codecs given some configuration.
 */
/**
 * Clients
 */
case class ClientCodecConfig(serviceName: String)

/**
 * Servers
 */
case class ServerCodecConfig(serviceName: String, boundAddress: SocketAddress) {
  def boundInetSocketAddress = boundAddress match {
    case ia: InetSocketAddress => ia
    case _ => new InetSocketAddress(0)
  }
}

/**
 * A combined codec factory provides both client and server codec
 * factories in one (when available).
 */
trait CodecFactory[Req, Rep] {
  type Client = ClientCodecConfig => Codec[Req, Rep]
  type Server = ServerCodecConfig => Codec[Req, Rep]

  def client: Client
  def server: Server

  /**
   * A protocol library name to use for displaying which protocol library this client or server is using.
   */
  def protocolLibraryName: String = "not-specified"

  /**
   * Converts this codec factory into a [[StackClient]].
   */
  def toStackClient: StackClient[Req, Rep] = CodecClient(client)

  /**
   * Converts this codec factory into a [[StackServer]].
   */
  def toStackServer: StackServer[Req, Rep] = CodecServer(server)
}

/**
 * A [[StackClient]] based on a [[Codec]].
 */
private case class CodecClient[Req, Rep](
  codecFactory: CodecFactory[Req, Rep]#Client,
  stack: Stack[ServiceFactory[Req, Rep]] = StackClient.newStack[Req, Rep],
  params: Stack.Params = Stack.Params.empty
) extends StackClient[Req, Rep] {

  import com.twitter.finagle.param._

  def withParams(ps: Stack.Params): StackClient[Req, Rep] = copy(params = ps)
  def withStack(stack: Stack[ServiceFactory[Req, Rep]]): StackClient[Req, Rep] = copy(stack = stack)

  def newClient(dest: Name, label: String): ServiceFactory[Req, Rep] = {
    val codec = codecFactory(ClientCodecConfig(label))

    val prepConn = new Stack.ModuleParams[ServiceFactory[Req, Rep]] {
      def parameters: Seq[Stack.Param[_]] = Nil
      val role: Stack.Role = StackClient.Role.prepConn
      val description = "Connection preparation phase as defined by a Codec"
      def make(ps: Stack.Params, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        val Stats(stats) = ps[Stats]
        val underlying = codec.prepareConnFactory(next, ps)
        new ServiceFactoryProxy(underlying) {
          private val stat = stats.stat("codec_connection_preparation_latency_ms")
          override def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
            val begin = Time.now
            super.apply(conn) ensure {
              stat.add((Time.now - begin).inMilliseconds)
            }
          }
        }
      }
    }

    val clientStack = {
      val stack0 = stack
        .replace(StackClient.Role.prepConn, prepConn)
        .replace(
          StackClient.Role.prepFactory,
          (next: ServiceFactory[Req, Rep]) => codec.prepareServiceFactory(next)
        )
        .replace(TraceInitializerFilter.role, codec.newTraceInitializer)

      // disable failFast if the codec requests it
      val FailFast(failFast) = params[FailFast]
      if (!codec.failFastOk || !failFast) stack0.remove(FailFastFactory.role) else stack0
    }

    case class Underlying(
      stack: Stack[ServiceFactory[Req, Rep]] = clientStack,
      params: Stack.Params = params
    ) extends StdStackClient[Req, Rep, Underlying] {

      protected def copy1(
        stack: Stack[ServiceFactory[Req, Rep]] = this.stack,
        params: Stack.Params = this.params
      ): Underlying = copy(stack, params)

      protected type In = Any
      protected type Out = Any
      protected type Context = TransportContext

      protected def newTransporter(addr: SocketAddress): Transporter[Any, Any, TransportContext] = {
        val Stats(stats) = params[Stats]
        val newTransport = (ch: Channel) => codec.newClientTransport(ch, stats)
        Netty3Transporter[Any, Any](
          codec.pipelineFactory,
          addr,
          params + Netty3Transporter.TransportFactory(newTransport)
        )
      }

      protected def newDispatcher(transport: Transport[In, Out] { type Context <: TransportContext })
        : Service[Req, Rep] =
        codec.newClientDispatcher(transport, params)
    }

    val proto = params[ProtocolLibrary]

    // don't override a configured protocol value
    val clientParams =
      if (proto != ProtocolLibrary.param.default) params
      else params + ProtocolLibrary(codec.protocolLibraryName)

    Underlying(clientStack, clientParams).newClient(dest, label)
  }

  def newService(dest: Name, label: String): Service[Req, Rep] = {
    val client = withParams(params + FactoryToService.Enabled(true)).newClient(dest, label)
    new FactoryToService[Req, Rep](client)
  }
}

private case class CodecServer[Req, Rep](
  codecFactory: CodecFactory[Req, Rep]#Server,
  stack: Stack[ServiceFactory[Req, Rep]] = StackServer.newStack[Req, Rep],
  params: Stack.Params = Stack.Params.empty
) extends StackServer[Req, Rep] {

  def withStack(stack: Stack[ServiceFactory[Req, Rep]]): StackServer[Req, Rep] = copy(stack = stack)
  def withParams(ps: Stack.Params): StackServer[Req, Rep] = copy(params = ps)

  def serve(addr: SocketAddress, service: ServiceFactory[Req, Rep]): ListeningServer = {

    val Label(label) = params[Label]
    val Stats(stats) = params[Stats]
    val codec = codecFactory(ServerCodecConfig(label, addr))

    val serverStack = stack
      .replace(
        StackServer.Role.preparer,
        (next: ServiceFactory[Req, Rep]) =>
          codec.prepareConnFactory(next, params + Stats(stats.scope(label)))
      )
      .replace(TraceInitializerFilter.role, codec.newTraceInitializer)

    val proto = params[ProtocolLibrary]
    val serverParams =
      if (proto != ProtocolLibrary.param.default) params
      else params + ProtocolLibrary(codec.protocolLibraryName)

    case class Underlying(
      stack: Stack[ServiceFactory[Req, Rep]] = serverStack,
      params: Stack.Params = serverParams
    ) extends StdStackServer[Req, Rep, Underlying] {

      protected type In = Any
      protected type Out = Any
      protected type Context = TransportContext

      protected def copy1(
        stack: Stack[ServiceFactory[Req, Rep]] = this.stack,
        params: Stack.Params = this.params
      ): Underlying = copy(stack, params)

      protected def newListener(): Listener[Any, Any, TransportContext] =
        Netty3Listener(codec.pipelineFactory, params)

      protected def newDispatcher(
        transport: Transport[In, Out] { type Context <: TransportContext },
        service: Service[Req, Rep]
      ): Closable = codec.newServerDispatcher(transport, service)
    }

    Underlying(serverStack, serverParams).serve(addr, service)
  }
}
