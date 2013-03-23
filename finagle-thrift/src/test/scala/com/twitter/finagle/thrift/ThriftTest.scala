package com.twitter.finagle.thrift

import com.twitter.finagle._
import com.twitter.finagle.builder.{ServerBuilder, ClientBuilder}
import com.twitter.finagle.thrift._
import com.twitter.finagle.tracing.{DefaultTracer, BufferingTracer}
import java.net.{SocketAddress, InetSocketAddress}
import org.apache.thrift.protocol._
import org.scalatest.FunSuite
import scala.collection.mutable

/**
 * A test mixin to test all combinations of servers, clients and protocols.
 */
trait ThriftTest { self: FunSuite =>
  type Iface <: AnyRef
  def ifaceManifest: ClassManifest[Iface]
  val processor: Iface
  val ifaceToService: (Iface, TProtocolFactory) => Service[Array[Byte], Array[Byte]]
  val serviceToIface: (Service[ThriftClientRequest, Array[Byte]], TProtocolFactory) => Iface

  private val thriftTests =
    mutable.Map[String, (Iface, BufferingTracer) => Unit]()

  /**
   * Define a new thrift test, which is run over the cross product
   * of known thrift configurations. Run when `runThriftTests` is
   * invoked.
   */
  def testThrift(label: String)(theTest: (Iface, BufferingTracer) => Unit) {
    thriftTests += (label -> theTest)
  }

  private val newBuilderServer = (protocolFactory: TProtocolFactory) => new {
    val server = ServerBuilder()
      .codec(ThriftServerFramedCodec(protocolFactory))
      .bindTo(new InetSocketAddress(0))
      .name("thriftserver")
      .tracer(DefaultTracer)
      .build(ifaceToService(processor, protocolFactory))

    val boundAddr = server.localAddress

    def close() {
      server.close()
    }
  }

  private val newBuilderClient = (protocolFactory: TProtocolFactory, addr: SocketAddress) =>
    new {
      val serviceFactory = ClientBuilder()
        .hosts(Seq(addr))
        .codec(ThriftClientFramedCodec().protocolFactory(protocolFactory))
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
    val server = Thrift
      .withProtocolFactory(protocolFactory)
      .serveIface("thriftserver=:*", processor)
    val boundAddr = server.boundAddress

    def close() {
      server.close()
    }
  }

  private val newAPIClient = (protocolFactory: TProtocolFactory, addr: SocketAddress) =>
    new {
      implicit val cls = ifaceManifest
      val client = Thrift
        .withProtocolFactory(protocolFactory)
        .newIface[Iface](Group(addr).named("thriftclient"))

      def close() = ()
    }

  private val protocols = Map(
    "binary" -> new TBinaryProtocol.Factory(),
    "compact" -> new TCompactProtocol.Factory(),
    "json" -> new TJSONProtocol.Factory()
  )

  // For some reason, the compiler needs some help here.
  private type NewClient = (TProtocolFactory, SocketAddress) => {
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
    (label, thriftTest) <- thriftTests
  } test("server:%s client:%s proto:%s %s".format(
    clientName, serverName, protoName, label)) {
    val tracer = new BufferingTracer
    val previous = DefaultTracer.self
    DefaultTracer.self = tracer
    val server = newServer(proto)
    val client = newClient(proto, server.boundAddr)
    try thriftTest(client.client, tracer) finally {
      DefaultTracer.self = previous
      server.close()
      client.close()
    }
  }
}

/*
  p.s. The very complexity of the above code should be enough to
  convince anyone of Thrift's hazardous attitude towards software
  modularity and proper layering.
*/
