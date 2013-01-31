package com.twitter.finagle.protobuf.rpc


import java.util.concurrent._
import java.util.concurrent.atomic._

object RpcProtobufSpec extends SpecificationWithJUnit {

    def CLIENT_TIMEOUT_SECONDS = 1

    def THREAD_COUNT = 40

    def REQ_PER_THREAD = 100

    def port = 8080

    def executorService = Executors.newFixedThreadPool(4)

    def factory = new RpcFactoryImpl()

    def serverBuilder = ServerBuilder.get().maxConcurrentRequests(10)

    def clientBuilder = ClientBuilder
            .get()
            .hosts(String.format("localhost:%s", port.toString()))
            .hostConnectionLimit(1)
            .retries(2)
            .requestTimeout(Duration(CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS))

  "A client" should {

        val totalRequests = new AtomicInteger()

        val service = new SampleWeatherServiceImpl(80, null)
        val server = factory.createServer(serverBuilder.asInstanceOf[ServerBuilder[(String, com.google.protobuf.Message),(String, com.google.protobuf.Message),Any,Any,Any]], port, service, executorService)
        val stub = factory.createStub(clientBuilder.asInstanceOf[ClientBuilder[(String, com.google.protobuf.Message),(String, com.google.protobuf.Message),Any,Any,Any]], WeatherService.newStub(null).asInstanceOf[{ def newStub(c: RpcChannel): WeatherService }], executorService)

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

      "receive THREAD_COUNT * REQ_PER_THREAD responses." in {
        THREAD_COUNT * REQ_PER_THREAD mustEqual totalRequests.get()
      }

  }


  def makeRequest(service: SampleWeatherServiceImpl, stub: WeatherService, totalRequests: AtomicInteger) {
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


class SampleWeatherServiceImpl(val temperature: Int, val getHistoricWeather: Callable[Any]) extends WeatherService {

        def getTemperature() = temperature

        def getWeatherForecast(controller: RpcController, request: GetWeatherForecastRequest, done:
                RpcCallback[GetWeatherForecastResponse])  {
            done.run(GetWeatherForecastResponse.newBuilder().setTemp(temperature).build())
        }

        def getHistoricWeather(controller: RpcController, request: GetHistoricWeatherRequest,
                done: RpcCallback[GetHistoricWeatherResponse]) {
            if (getHistoricWeather != null) {
                getHistoricWeather.call()
            }
        }

}
