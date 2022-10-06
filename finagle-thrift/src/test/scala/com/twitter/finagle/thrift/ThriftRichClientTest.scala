package com.twitter.finagle.thrift

import com.twitter.finagle.Filter.TypeAgnostic
import com.twitter.finagle._
import com.twitter.finagle.client.StackBasedClient
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.thrift.service.Filterable
import com.twitter.finagle.thrift.service.ServicePerEndpointBuilder
import org.apache.thrift.protocol.TProtocolFactory
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

class ThriftRichClientTest extends AnyFunSuite with MockitoSugar {
  class MockClient(val params: Stack.Params)(onNewService: MockClient => Unit)
      extends StackBasedClient[ThriftClientRequest, Array[Byte]]
      with ThriftRichClient {

    def transformed(t: Stack.Transformer): StackBasedClient[ThriftClientRequest, Array[Byte]] = this
    def withParams(ps: Stack.Params): StackBasedClient[ThriftClientRequest, Array[Byte]] =
      new MockClient(ps)(onNewService)

    lazy val clientParam = RichClientParam(Protocols.binaryFactory())

    protected val protocolFactory: TProtocolFactory = clientParam.protocolFactory
    override protected val stats: StatsReceiver = clientParam.clientStats

    override val defaultClientName = "mock_client"

    def newService(dest: Name, label: String): Service[ThriftClientRequest, Array[Byte]] = {
      onNewService(this)
      mock[Service[ThriftClientRequest, Array[Byte]]]
    }

    def newClient(dest: Name, label: String): ServiceFactory[ThriftClientRequest, Array[Byte]] = {
      onNewService(this)
      mock[ServiceFactory[ThriftClientRequest, Array[Byte]]]
    }
  }

  private class SvcIface extends Filterable[SvcIface] {
    def filtered(filter: TypeAgnostic): SvcIface = this
  }

  private object SvcIface {
    implicit val serviceIfaceBuilder: ServiceIfaceBuilder[SvcIface] =
      new ServiceIfaceBuilder[SvcIface] {
        override def serviceClass: Class[SvcIface] = classOf[SvcIface]
        def newServiceIface(
          thriftService: Service[ThriftClientRequest, Array[Byte]],
          clientParam: RichClientParam
        ): SvcIface = new SvcIface
      }

    implicit val servicePerEndpointBuilder: ServicePerEndpointBuilder[SvcIface] =
      new ServicePerEndpointBuilder[SvcIface] {
        override def serviceClass: Class[SvcIface] = classOf[SvcIface]
        def servicePerEndpoint(
          thriftService: Service[ThriftClientRequest, Array[Byte]],
          clientParam: RichClientParam
        ): SvcIface = new SvcIface
      }
  }

  private val svcIface = new SvcIface

  test(
    "ThriftRichClientTest servicePerEndpoint takes dest String and stats scoping label arguments"
  ) {
    val captor = ArgumentCaptor.forClass(classOf[RichClientParam])
    val mockBuilder = mock[ServicePerEndpointBuilder[SvcIface]]
    doReturn(svcIface, Nil: _*).when(mockBuilder).servicePerEndpoint(any(), captor.capture())
    val client = spy(new MockClient(Stack.Params.empty)(_ => ()))
    doReturn(client, Nil: _*).when(client).withParams(any[Stack.Params])
    client.servicePerEndpoint("dest_string", "client")(builder = mockBuilder)

    assert(captor.getValue.clientStats.toString == "NullStatsReceiver/clnt/client")
    verify(client).newService("dest_string", "client")
  }

  test(
    "ThriftRichClientTest servicePerEndpoint takes dest Name and stats scoping label arguments") {
    val captor = ArgumentCaptor.forClass(classOf[RichClientParam])
    val mockBuilder = mock[ServicePerEndpointBuilder[SvcIface]]
    doReturn(svcIface, Nil: _*).when(mockBuilder).servicePerEndpoint(any(), captor.capture())

    val name = Name.empty
    val client = spy(new MockClient(Stack.Params.empty)(_ => ()))
    doReturn(client, Nil: _*).when(client).withParams(any[Stack.Params])
    client.servicePerEndpoint(name, "client")(builder = mockBuilder)

    assert(captor.getValue.clientStats.toString == "NullStatsReceiver/clnt/client")
    verify(client).newService(name, "client")
  }

  test("newServiceIface captures the service class") {
    val client = new MockClient(Stack.Params.empty)(created =>
      assert(created.params[Thrift.param.ServiceClass].clazz == Some(classOf[SvcIface])))

    client.newServiceIface[SvcIface](Name.empty, "foo")
  }

  test("servicePerEndpoint captures the service class") {
    val client = new MockClient(Stack.Params.empty)(created =>
      assert(created.params[Thrift.param.ServiceClass].clazz == Some(classOf[SvcIface])))

    client.servicePerEndpoint[SvcIface](Name.empty, "foo")
  }

  test("newIface captures the service class") {
    val client = spy(new MockClient(Stack.Params.empty)(created =>
      assert(created.params[Thrift.param.ServiceClass].clazz == Some(classOf[SvcIface]))))

    doReturn(svcIface, Nil: _*)
      .when(client)
      .newIface(
        any[Name],
        anyString(),
        any[Class[_]],
        any[RichClientParam],
        any[Service[ThriftClientRequest, Array[Byte]]])

    client.newIface[SvcIface](Name.empty, "foo")
  }

  test("build captures the service class") {
    val client = spy(new MockClient(Stack.Params.empty)(created =>
      assert(created.params[Thrift.param.ServiceClass].clazz == Some(classOf[SvcIface]))))

    doReturn(svcIface, Nil: _*)
      .when(client)
      .build(
        any[Name],
        anyString(),
        any[Class[_]],
        any[RichClientParam],
        any[Service[ThriftClientRequest, Array[Byte]]])

    client.build[SvcIface](Name.empty, "foo")
  }
}
