package com.twitter.finagle.example.thrift

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.example.thriftscala._
import com.twitter.finagle.service.{RetryExceptionsFilter, RetryPolicy, TimeoutFilter}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Await, Duration, Future, Throw, Try}

object ThriftServicePerEndpointExample {
  def main(args: Array[String]): Unit = {
    // See the docs at https://twitter.github.io/finagle/guide/Protocols.html#using-finagle-thrift
    //#thriftserverapi
    val server: ListeningServer = Thrift.server.serveIface(
      "localhost:1234",
      new LoggerService.MethodPerEndpoint {
        def log(message: String, logLevel: Int): Future[String] = {
          println(s"[$logLevel] Server received: '$message'")
          Future.value(s"You've sent: ('$message', $logLevel)")
        }

        var counter = 0
        // getLogSize throws ReadExceptions every other request.
        def getLogSize(): Future[Int] = {
          counter += 1
          if (counter % 2 == 1) {
            println(s"Server: getLogSize ReadException")
            Future.exception(new ReadException())
          } else {
            println(s"Server: getLogSize Success")
            Future.value(4)
          }
        }
      }
    )
    //#thriftserverapi

    import LoggerService._

    //#thriftclientapi
    val clientServicePerEndpoint: LoggerService.ServicePerEndpoint =
      Thrift.client.servicePerEndpoint[LoggerService.ServicePerEndpoint](
        "localhost:1234",
        "thrift_client"
      )
    //#thriftclientapi

    //#thriftclientapi-call
    val result: Future[Log.SuccessType] = clientServicePerEndpoint.log(Log.Args("hello", 1))
    //#thriftclientapi-call

    Await.result(result)

    //#thriftclientapi-filters
    val uppercaseFilter = new SimpleFilter[Log.Args, Log.SuccessType] {
      def apply(
        req: Log.Args,
        service: Service[Log.Args, Log.SuccessType]
      ): Future[Log.SuccessType] = {
        val uppercaseRequest = req.copy(message = req.message.toUpperCase)
        service(uppercaseRequest)
      }
    }

    def timeoutFilter[Req, Rep](duration: Duration) = {
      val exc = new IndividualRequestTimeoutException(duration)
      val timer = DefaultTimer
      new TimeoutFilter[Req, Rep](duration, exc, timer)
    }
    val filteredLog: Service[Log.Args, Log.SuccessType] = timeoutFilter(2.seconds)
      .andThen(uppercaseFilter)
      .andThen(clientServicePerEndpoint.log)

    filteredLog(Log.Args("hello", 2))
    // [2] Server received: 'HELLO'
    //#thriftclientapi-filters

    //#thriftclientapi-retries
    val retryPolicy: RetryPolicy[Try[GetLogSize.Result]] =
      RetryPolicy.tries[Try[GetLogSize.Result]](
        3,
        {
          case Throw(ex: ReadException) => true
        })

    val retriedGetLogSize: Service[GetLogSize.Args, GetLogSize.SuccessType] =
      new RetryExceptionsFilter(retryPolicy, DefaultTimer)
        .andThen(clientServicePerEndpoint.getLogSize)

    retriedGetLogSize(GetLogSize.Args())
    //#thriftclientapi-retries

    //#thriftclientapi-methodiface
    val client: LoggerService.MethodPerEndpoint =
      Thrift.client.build[LoggerService.MethodPerEndpoint]("localhost:1234")
    client.log("message", 4).onSuccess { response =>
      println("Client received response: " + response)
    }
    //#thriftclientapi-methodiface

    //#thriftclientapi-method-adapter
    val filteredMethodIface: LoggerService.MethodPerEndpoint =
      Thrift.Client.methodPerEndpoint(clientServicePerEndpoint.withLog(filteredLog))
    Await.result(filteredMethodIface.log("ping", 3).map(println))
    //#thriftclientapi-method-adapter
  }
}
