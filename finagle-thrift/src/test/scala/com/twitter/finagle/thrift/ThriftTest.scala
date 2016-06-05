package com.twitter.finagle.thrift

import com.twitter.finagle._
import com.twitter.finagle.builder.{ServerBuilder, ClientBuilder}
import com.twitter.finagle.tracing.{DefaultTracer, BufferingTracer, Trace}
import java.net.{SocketAddress, InetSocketAddress, InetAddress}
import org.apache.thrift.protocol._
import org.scalatest.FunSuite
import scala.collection.mutable
import scala.language.reflectiveCalls
import scala.reflect.ClassTag

/**
 * A test mixin to test all combinations of servers, clients and protocols.
 */
trait ThriftTest { self: FunSuite =>
  type Iface <: AnyRef
  def ifaceManifest: ClassTag[Iface]
  val processor: Iface
  val ifaceToService: (Iface, TProtocolFactory) => Service[Array[Byte], Array[Byte]]
  val serviceToIface: (Service[ThriftClientRequest, Array[Byte]], TProtocolFactory) => Iface
  val loopback = InetAddress.getLoopbackAddress

  /**
   * A struct defining the data needed to run a single Thrift test.
   */
  case class ThriftTestDefinition(
    label: String,
    clientIdOpt: Option[ClientId],
    testFunction: ((Iface, BufferingTracer) => Unit)
  )

  private val thriftTests = mutable.ListBuffer[ThriftTestDefinition]()

  /**
   * Define a new thrift test, which is run over the cross product
   * of known thrift configurations. Run when `runThriftTests` is
   * invoked.
   */
  def testThrift(
    label: String,
    clientIdOpt: Option[ClientId] = None
  )(theTest: (Iface, BufferingTracer) => Unit) {
    thriftTests += ThriftTestDefinition(label, clientIdOpt, theTest)
  }

  def skipTestThrift(
    label: String,
    clientIdOpt: Option[ClientId] = None
  )(theTest: (Iface, BufferingTracer) => Unit) {
    () // noop
  }

  private val newBuilderServer = (protocolFactory: TProtocolFactory) => new {
    val server = ServerBuilder()
      .codec(ThriftServerFramedCodec(protocolFactory))
      .bindTo(new InetSocketAddress(loopback, 0))
      .name("thriftserver")
      .tracer(DefaultTracer)
      .build(ifaceToService(processor, protocolFactory))

    val boundAddr = server.boundAddress

    def close() {
      server.close()
    }
  }

  private val newBuilderClient = (
    protocolFactory: TProtocolFactory,
    addr: SocketAddress,
    clientIdOpt: Option[ClientId]
  ) => new {
    val serviceFactory = ClientBuilder()
      .hosts(Seq(addr.asInstanceOf[InetSocketAddress]))
      .codec(ThriftClientFramedCodec(clientIdOpt).protocolFactory(protocolFactory))
      .name("thriftclient")
      .hostConnectionLimit(2)
      .tracer(DefaultTracer)
      .buildFactory()
    val service = serviceFactory.toService
    val client = serviceToIface(service, protocolFactory)

    def close() {
      service.close()
    }
  }

  private val newAPIServer = (protocolFactory: TProtocolFactory) => new {
    val server = Thrift.server
      .withLabel("thriftserver")
      .withProtocolFactory(protocolFactory)
      .serveIface("localhost:*", processor)
    val boundAddr = server.boundAddress

    def close() {
      server.close()
    }
  }

  private val newAPIClient = (
    protocolFactory: TProtocolFactory,
    addr: SocketAddress,
    clientIdOpt: Option[ClientId]
  ) => new {
    implicit val cls = ifaceManifest
    val client = {
      val thrift = clientIdOpt.foldLeft(Thrift.client.withProtocolFactory(protocolFactory)) {
        case (thrift, clientId) => thrift.withClientId(clientId)
      }

      thrift.newIface[Iface](Group(addr).named("thriftclient"))
    }

    def close() = ()
  }

  private val protocols = Map(
    // Commenting out due to flakiness - see DPT-175 and DPT-181
    "binary" -> new TBinaryProtocol.Factory()
//    "compact" -> new TCompactProtocol.Factory()
// Unsupported. Add back when we upgrade Thrift.
// (There's a test that will fail when we do.)
//    "json" -> new TJSONProtocol.Factory()
  )

  // For some reason, the compiler needs some help here.
  private type NewClient = (TProtocolFactory, SocketAddress, Option[ClientId]) => {
    def close()
    val client: Iface
  }

  private type NewServer = (TProtocolFactory) => {
    def close()
    val boundAddr: SocketAddress
  }

  private val clients = Map[String, NewClient](
    "builder" -> newBuilderClient,
    "api" -> newAPIClient
  )

  private val servers = Map[String, NewServer](
    "builder" -> newBuilderServer,
    "api" -> newAPIServer
  )

  /** Invoke this in your test to run all defined thrift tests */
  def runThriftTests() = for {
    (protoName, proto) <- protocols
    (clientName, newClient) <- clients
    (serverName, newServer) <- servers
    testDef <- thriftTests
  } test("server:%s client:%s proto:%s %s".format(
    serverName, clientName, protoName, testDef.label)) {
    val tracer = new BufferingTracer
    val previous = DefaultTracer.self
    DefaultTracer.self = tracer
    val server = newServer(proto)
    val client = newClient(proto, server.boundAddr, testDef.clientIdOpt)
    Trace.letClear {
      try testDef.testFunction(client.client, tracer) finally {
        DefaultTracer.self = previous
        server.close()
        client.close()
      }
    }
  }
}

/*
  p.s. The very complexity of the above code should be enough to
  convince anyone of Thrift's hazardous attitude towards software
  modularity and proper layering.
*/
