package com.twitter.finagle.protobuf.rpc

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

import org.junit.Assert._

import com.google.protobuf._
import com.google.common.base._

import com.twitter.util._
import com.twitter.finagle.protobuf.rpc._
import com.twitter.finagle.protobuf.rpc.impl._

import com.twitter.finagle.protobuf.rpc.SampleServiceProtos._
import com.twitter.finagle.builder._

import java.util.concurrent._
import java.util.concurrent.atomic._

import org.slf4j.LoggerFactory

import com.twitter.finagle.protobuf.rpc.ServiceExceptionHandler

object Config {
  def firstPort = 8080

  def secondPort = firstPort + 1

  def CLIENT_TIMEOUT_SECONDS = 1

  def factory = new RpcFactoryImpl()

  val threadPool = Executors.newFixedThreadPool(1)

  def executorService() = threadPool

  def clientBuilder = ClientBuilder
    .get()
    .hosts(String.format("localhost:%s", Config.firstPort.toString()))
    .hostConnectionLimit(1)
    .retries(2)
    .requestTimeout(Duration(Config.CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS))

  def reqCount = 10
}

object RpcChainSpec extends org.specs.SpecificationWithJUnit {
  val serverBuilder = ServerBuilder.get().maxConcurrentRequests(10)


  "A client" should {

    val respReceived = new AtomicInteger(0);
    val failedRespReceived = new AtomicInteger(0);

    // first service
    val firstService = new FirstWeatherServiceImpl()
    val firstServer = Config.factory.createServer(serverBuilder.asInstanceOf[ServerBuilder[(String, com.google.protobuf.Message),
      (String, com.google.protobuf.Message), Any, Any, Any]], Config.firstPort, firstService,
      new ServiceExceptionHandler[Message] {

        def canHandle(e: RuntimeException) = false

        def handle(e: RuntimeException, m: Message) = null

      },
      Config.executorService)

    // second service
    val secondService = new SecondWeatherServiceImpl()
    val secondServer = Config.factory.createServer(serverBuilder.asInstanceOf[ServerBuilder[(String, com.google.protobuf.Message),
      (String, com.google.protobuf.Message), Any, Any, Any]], Config.secondPort, secondService,
      new ServiceExceptionHandler[Message] {

        def canHandle(e: RuntimeException) = false

        def handle(e: RuntimeException, m: Message) = null

      },
      Config.executorService)

    val stub = Config.factory.createStub(Config.clientBuilder.asInstanceOf[ClientBuilder[(String, com.google.protobuf.Message),
      (String, com.google.protobuf.Message), Any, Any, Any]],
      WeatherService.newStub(null).asInstanceOf[ {def newStub(c: RpcChannel): WeatherService}],
      new ExceptionResponseHandler[Message] {

        def canHandle(message: Message) = false

        def handle(message: Message) = null

      },
      Config.executorService)

    val latch = new java.util.concurrent.CountDownLatch(Config.reqCount)


    for (i <- 1 to Config.reqCount) {
      val request = GetWeatherForecastRequest.newBuilder().setZip(new Integer(i).toString).build()
      stub.getWeatherForecast(Config.factory.createController().asInstanceOf[RpcControllerWithOnFailureCallback].onFailure(new RpcCallback[Throwable]() {

        def run(e: Throwable) {
          failedRespReceived.incrementAndGet();
          latch.countDown();
        }
      }), request, new RpcCallback[GetWeatherForecastResponse]() {

        def run(resp: GetWeatherForecastResponse) {
          respReceived.incrementAndGet()
          latch.countDown();
        }
      });
    }

    latch.await(10l, TimeUnit.SECONDS)
    firstServer.close(Duration(1, TimeUnit.SECONDS))
    secondServer.close(Duration(1, TimeUnit.SECONDS))

    "receive a response" in {
      respReceived.get() + failedRespReceived.get() mustEqual Config.reqCount
    }
    "some requests succeeded" in {
      respReceived.get() must beGreaterThan(0)
    }
    "some requests failed" in {
      failedRespReceived.get() must beGreaterThan(0)
    }

  }

}


class FirstWeatherServiceImpl() extends WeatherService {

  def getWeatherForecast(controller: RpcController, request: GetWeatherForecastRequest, done:
  RpcCallback[GetWeatherForecastResponse]) {

    // we are a client for the second service
    val cb = Config.clientBuilder.hosts(String.format("localhost:%s", Config.secondPort.toString()))
    val stub = Config.factory.createStub(cb.asInstanceOf[ClientBuilder[(String, com.google.protobuf.Message),
      (String, com.google.protobuf.Message), Any, Any, Any]],
      WeatherService.newStub(null).asInstanceOf[ {def newStub(c: RpcChannel): WeatherService}],
      new ExceptionResponseHandler[Message] {

        def canHandle(message: Message) = false

        def handle(message: Message) = null

      },
      Config.executorService)

    stub.getWeatherForecast(Config.factory.createController().asInstanceOf[RpcControllerWithOnFailureCallback].onFailure(new RpcCallback[Throwable]() {

      def run(e: Throwable) {
      }
    }), request, new RpcCallback[GetWeatherForecastResponse]() {

      def run(resp: GetWeatherForecastResponse) {
        done.run(GetWeatherForecastResponse.newBuilder().setTemp(resp.getTemp()).build())
      }
    });


  }

  def getHistoricWeather(controller: RpcController, request: GetHistoricWeatherRequest,
                         done: RpcCallback[GetHistoricWeatherResponse]) {
    //not used
  }

}

class SecondWeatherServiceImpl() extends WeatherService {

  def getWeatherForecast(controller: RpcController, request: GetWeatherForecastRequest, done:
  RpcCallback[GetWeatherForecastResponse]) {
    if (Integer.parseInt(request.getZip()) % 3 == 0) return
    done.run(GetWeatherForecastResponse.newBuilder().setTemp(80).build())
  }

  def getHistoricWeather(controller: RpcController, request: GetHistoricWeatherRequest,
                         done: RpcCallback[GetHistoricWeatherResponse]) {
    //not used
  }

}
