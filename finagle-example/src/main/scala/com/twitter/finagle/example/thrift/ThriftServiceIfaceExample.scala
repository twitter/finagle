package com.twitter.finagle.example.thrift

import java.net.{InetAddress, InetSocketAddress}

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.example.thriftscala._
import com.twitter.finagle.service.{RetryExceptionsFilter, RetryPolicy, TimeoutFilter}
import com.twitter.finagle.thrift.ThriftServiceIface
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Await, Duration, Future, Throw, Try}

object ThriftServiceIfaceExample {
  def main(args: Array[String]) {
    // See the docs at http://twitter.github.io/finagle/guide/Protocols.html#using-finagle-thrift
    //#thriftserverapi
    val server = Thrift.serveIface(
      "localhost:1234",
      new LoggerService[Future] {
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
      })
    //#thriftserverapi

    import LoggerService._

    //#thriftclientapi
    val clientServiceIface: LoggerService.ServiceIface =
      Thrift.newServiceIface[LoggerService.ServiceIface]("localhost:1234")
    //#thriftclientapi

    //#thriftclientapi-call
    val result: Future[Log.Result] = clientServiceIface.log(Log.Args("hello", 1))
    //#thriftclientapi-call

    Await.result(result)

    //#thriftclientapi-filters
    val uppercaseFilter = new SimpleFilter[Log.Args, Log.Result] {
      def apply(req: Log.Args, service: Service[Log.Args, Log.Result]): Future[Log.Result] = {
        val uppercaseRequest = req.copy(message = req.message.toUpperCase)
        service(uppercaseRequest)
      }
    }

    def timeoutFilter[Req, Rep](duration: Duration) = {
      val exc = new IndividualRequestTimeoutException(duration)
      val timer = DefaultTimer.twitter
      new TimeoutFilter[Req, Rep](duration, exc, timer)
    }
    val filteredLog = timeoutFilter(2.seconds) andThen uppercaseFilter andThen clientServiceIface.log

    filteredLog(Log.Args("hello", 2))
    // [2] Server received: 'HELLO'
    //#thriftclientapi-filters

    //#thriftclientapi-retries
    val retryPolicy = RetryPolicy.tries[Try[GetLogSize.Result]](3,
    {
      case Throw(ex: ReadException) => true
    })

    val retriedGetLogSize =
      new RetryExceptionsFilter(retryPolicy, DefaultTimer.twitter) andThen
        ThriftServiceIface.resultFilter(GetLogSize) andThen
        clientServiceIface.getLogSize

    retriedGetLogSize(GetLogSize.Args())
    //#thriftclientapi-retries

    //#thriftclientapi-methodiface
    val client: LoggerService.FutureIface = Thrift.newIface[LoggerService.FutureIface]("localhost:1234")
    client.log("message", 4) onSuccess { response =>
      println("Client received response: " + response)
    }
    //#thriftclientapi-methodiface

    //#thriftclientapi-method-adapter
    val filteredMethodIface: LoggerService[Future] =
      Thrift.newMethodIface(clientServiceIface.copy(log = filteredLog))
    Await.result(filteredMethodIface.log("ping", 3).map(println))
    //#thriftclientapi-method-adapter
  }
}
