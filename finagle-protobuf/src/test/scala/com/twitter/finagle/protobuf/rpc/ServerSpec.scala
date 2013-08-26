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

import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.collection.mutable._

import com.twitter.finagle.protobuf.rpc.ServiceExceptionHandler

object RpcProtobufSpec extends org.specs.SpecificationWithJUnit {

  def CLIENT_TIMEOUT_SECONDS = 1

  def THREAD_COUNT = 40

  def REQ_PER_THREAD = 100

  def port = 8080

  def executorService = Executors.newFixedThreadPool(10, new ThreadFactoryBuilder().setNameFormat("#service-dispatch#-%d").build())

  def factory = new RpcFactoryImpl()

  def serverBuilder = ServerBuilder.get().maxConcurrentRequests(10)

  val handlingThreadNames =  new ListBuffer[String]()

  def clientBuilder = ClientBuilder
    .get()
    .hosts(String.format("localhost:%s", port.toString()))
    .hostConnectionLimit(1)
    .retries(2)
    .requestTimeout(Duration(CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS))

  "A server" should {

    val totalRequests = new AtomicInteger()

    val service = new SampleWeatherServiceImpl2(80, null, handlingThreadNames)

    val server = factory.createServer(serverBuilder.asInstanceOf[ServerBuilder[(String, com.google.protobuf.Message),
      (String, com.google.protobuf.Message), Any, Any, Any]], port, service,
      new ServiceExceptionHandler[Message] {

        def canHandle(e: RuntimeException) = false

        def handle(e: RuntimeException, m: Message) = null

      },
      executorService)
    val stub = factory.createStub(clientBuilder.asInstanceOf[ClientBuilder[(String, com.google.protobuf.Message),
      (String, com.google.protobuf.Message), Any, Any, Any]],
      WeatherService.newStub(null).asInstanceOf[ {def newStub(c: RpcChannel): WeatherService}],
      new ExceptionResponseHandler[Message] {

        def canHandle(message: Message) = false

        def handle(message: Message) = null

      },
      executorService)

    val finishBarrier = new CyclicBarrier(THREAD_COUNT + 1)
    val startBarrier = new CyclicBarrier(THREAD_COUNT)

    for (i <- 0 until THREAD_COUNT) {
      new Thread(new Runnable() {
        def run() {
          startBarrier.await();
          try {
            for (k <- 0 until REQ_PER_THREAD) {
              makeRequest(service, stub, totalRequests)
            }
          }
          finally {
            finishBarrier.await(60l, TimeUnit.SECONDS)
          }
        }
      }).start()
    }
    finishBarrier.await(60l, TimeUnit.SECONDS)
    server.close(Duration(1, TimeUnit.SECONDS))

    var requestHandledByServiceDispatch = 0
    handlingThreadNames.foreach((name: String) => if (name.startsWith("#service-dispatch#")) requestHandledByServiceDispatch += 1 )

    "receive THREAD_COUNT * REQ_PER_THREAD responses." in {
      THREAD_COUNT * REQ_PER_THREAD mustEqual requestHandledByServiceDispatch
    }

  }


  def makeRequest(service: SampleWeatherServiceImpl2, stub: WeatherService, totalRequests: AtomicInteger) {
    val controller = factory.createController().asInstanceOf[RpcControllerWithOnFailureCallback]

    val l = new java.util.concurrent.CountDownLatch(1);
    val request = GetWeatherForecastRequest.newBuilder().setZip("80301").build()
    stub.getWeatherForecast(controller.onFailure(new RpcCallback[Throwable]() {

      def run(e: Throwable) {
        l.countDown()
      }
    }), request, new RpcCallback[GetWeatherForecastResponse]() {

      def run(resp: GetWeatherForecastResponse) {
        totalRequests.incrementAndGet()
        l.countDown()
      }
    });

    l.await(CLIENT_TIMEOUT_SECONDS + 2, TimeUnit.SECONDS)
  }
}


private class SampleWeatherServiceImpl2(val temperature: Int, val getHistoricWeather: Callable[Any], val handlingThreadNames: ListBuffer[String]) extends WeatherService {

  def getTemperature() = temperature

  def getWeatherForecast(controller: RpcController, request: GetWeatherForecastRequest, done:
  RpcCallback[GetWeatherForecastResponse]) {
    handlingThreadNames += Thread.currentThread().getName()
    done.run(GetWeatherForecastResponse.newBuilder().setTemp(temperature).build())
  }

  def getHistoricWeather(controller: RpcController, request: GetHistoricWeatherRequest,
                         done: RpcCallback[GetHistoricWeatherResponse]) {
    if (getHistoricWeather != null) {
      getHistoricWeather.call()
    }
  }

}
