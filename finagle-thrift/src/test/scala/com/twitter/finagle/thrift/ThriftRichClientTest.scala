package com.twitter.finagle.thrift

import com.twitter.finagle._
import com.twitter.finagle.stats.StatsReceiver

import org.apache.thrift.protocol.TProtocolFactory
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.{OneInstancePerTest, FunSuite}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar


@RunWith(classOf[JUnitRunner])
class ThriftRichClientTest extends FunSuite with MockitoSugar with OneInstancePerTest {
  object ThriftRichClientMock extends Client[ThriftClientRequest, Array[Byte]] with ThriftRichClient {
    override val protocolFactory: TProtocolFactory = Protocols.binaryFactory()
    override val defaultClientName = "mock_client"

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


  test("ThriftRichClientTest newServiceIface takes dest String and stats scoping label arguments") {
    val captor = ArgumentCaptor.forClass(classOf[StatsReceiver])
    val mockBuilder = mock[ServiceIfaceBuilder[String]]
    doReturn("mockServiceIface").when(mockBuilder).newServiceIface(any(), any(), captor.capture())

    val client = spy(ThriftRichClientMock)
    client.newServiceIface("/s/tweetypie/tweetypie", "tweetypie_client")(builder = mockBuilder)

    assert(captor.getValue.toString == "NullStatsReceiver/clnt/tweetypie_client")
    verify(client).newService("/s/tweetypie/tweetypie", "tweetypie_client")
  }

  test("ThriftRichClientTest newServiceIface takes dest Name and stats scoping label arguments") {
    val captor = ArgumentCaptor.forClass(classOf[StatsReceiver])
    val mockBuilder = mock[ServiceIfaceBuilder[String]]
    doReturn("mockServiceIface").when(mockBuilder).newServiceIface(any(), any(), captor.capture())

    val name = Name.empty
    val client = spy(ThriftRichClientMock)
    client.newServiceIface(name, "tweetypie_client")(builder = mockBuilder)

    assert(captor.getValue.toString == "NullStatsReceiver/clnt/tweetypie_client")
    verify(client).newService(name, "tweetypie_client")
  }
}
