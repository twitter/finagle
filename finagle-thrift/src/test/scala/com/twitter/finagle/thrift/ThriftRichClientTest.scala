package com.twitter.finagle.thrift

import com.twitter.finagle.Filter.TypeAgnostic
import com.twitter.finagle._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.thrift.service.{Filterable, ServicePerEndpointBuilder}
import org.apache.thrift.protocol.TProtocolFactory
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.{FunSuite, OneInstancePerTest}
import org.scalatestplus.mockito.MockitoSugar

class ThriftRichClientTest extends FunSuite with MockitoSugar with OneInstancePerTest {
  object ThriftRichClientMock
      extends Client[ThriftClientRequest, Array[Byte]]
      with ThriftRichClient {

    lazy val clientParam = RichClientParam(Protocols.binaryFactory())

    protected val protocolFactory: TProtocolFactory = clientParam.protocolFactory
    override protected val stats: StatsReceiver = clientParam.clientStats

    override val defaultClientName = "mock_client"

    protected def params: Stack.Params = Stack.Params.empty

    def newService(dest: Name, label: String): Service[ThriftClientRequest, Array[Byte]] =
      mock[Service[ThriftClientRequest, Array[Byte]]]

    def newClient(dest: Name, label: String): ServiceFactory[ThriftClientRequest, Array[Byte]] =
      mock[ServiceFactory[ThriftClientRequest, Array[Byte]]]
  }

  private class SvcIface extends Filterable[SvcIface] {
    def filtered(filter: TypeAgnostic): SvcIface = this
  }
  private val svcIface = new SvcIface

  test(
    "ThriftRichClientTest servicePerEndpoint takes dest String and stats scoping label arguments"
  ) {
    val captor = ArgumentCaptor.forClass(classOf[RichClientParam])
    val mockBuilder = mock[ServicePerEndpointBuilder[SvcIface]]
    doReturn(svcIface).when(mockBuilder).servicePerEndpoint(any(), captor.capture())
    val client = spy(ThriftRichClientMock)
    client.servicePerEndpoint("dest_string", "client")(builder = mockBuilder)

    assert(captor.getValue.clientStats.toString == "NullStatsReceiver/clnt/client")
    verify(client).newService("dest_string", "client")
  }

  test("ThriftRichClientTest servicePerEndpoint takes dest Name and stats scoping label arguments") {
    val captor = ArgumentCaptor.forClass(classOf[RichClientParam])
    val mockBuilder = mock[ServicePerEndpointBuilder[SvcIface]]
    doReturn(svcIface).when(mockBuilder).servicePerEndpoint(any(), captor.capture())

    val name = Name.empty
    val client = spy(ThriftRichClientMock)
    client.servicePerEndpoint(name, "client")(builder = mockBuilder)

    assert(captor.getValue.clientStats.toString == "NullStatsReceiver/clnt/client")
    verify(client).newService(name, "client")
  }
}
