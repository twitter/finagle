package com.twitter.finagle.thrift

import com.twitter.finagle.Filter.TypeAgnostic
import com.twitter.finagle._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.thrift.ThriftServiceIface.Filterable
import org.apache.thrift.protocol.TProtocolFactory
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.{FunSuite, OneInstancePerTest}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mockito.MockitoSugar


@RunWith(classOf[JUnitRunner])
class ThriftRichClientTest extends FunSuite with MockitoSugar with OneInstancePerTest {
  object ThriftRichClientMock extends Client[ThriftClientRequest, Array[Byte]] with ThriftRichClient {
    override val protocolFactory: TProtocolFactory = Protocols.binaryFactory()
    override val defaultClientName = "mock_client"

    protected def params: Stack.Params = Stack.Params.empty

    def newService(
      dest: Name,
      label: String
    ): Service[ThriftClientRequest, Array[Byte]] =
      mock[Service[ThriftClientRequest, Array[Byte]]]

    def newClient(
      dest: Name,
      label: String
    ): ServiceFactory[ThriftClientRequest, Array[Byte]] =
      mock[ServiceFactory[ThriftClientRequest, Array[Byte]]]
  }

  private class SvcIface extends Filterable[SvcIface] {
    def filtered(filter: TypeAgnostic): SvcIface = this
  }
  private val svcIface = new SvcIface

  test("ThriftRichClientTest newServiceIface takes dest String and stats scoping label arguments") {
    val captor = ArgumentCaptor.forClass(classOf[StatsReceiver])
    val mockBuilder = mock[ServiceIfaceBuilder[SvcIface]]
    doReturn(svcIface).when(mockBuilder).newServiceIface(any(), any(), captor.capture())

    val client = spy(ThriftRichClientMock)
    client.newServiceIface("/s/tweetypie/tweetypie", "tweetypie_client")(builder = mockBuilder)

    assert(captor.getValue.toString == "NullStatsReceiver/clnt/tweetypie_client")
    verify(client).newService("/s/tweetypie/tweetypie", "tweetypie_client")
  }

  test("ThriftRichClientTest newServiceIface takes dest Name and stats scoping label arguments") {
    val captor = ArgumentCaptor.forClass(classOf[StatsReceiver])
    val mockBuilder = mock[ServiceIfaceBuilder[SvcIface]]
    doReturn(svcIface).when(mockBuilder).newServiceIface(any(), any(), captor.capture())

    val name = Name.empty
    val client = spy(ThriftRichClientMock)
    client.newServiceIface(name, "tweetypie_client")(builder = mockBuilder)

    assert(captor.getValue.toString == "NullStatsReceiver/clnt/tweetypie_client")
    verify(client).newService(name, "tweetypie_client")
  }
}
