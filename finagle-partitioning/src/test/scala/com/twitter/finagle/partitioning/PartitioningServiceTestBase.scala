package com.twitter.finagle.partitioning

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.liveness.FailureAccrualFactory
import com.twitter.finagle.naming.BindingFactory
import com.twitter.finagle.param.Stats
import com.twitter.finagle.partitioning.PartitioningService.PartitionedResults
import com.twitter.finagle.partitioning.{param => partitioningParam}
import com.twitter.finagle.server.utils.StringServer
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{Address, param => ctfparam, _}
import com.twitter.util._
import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.CountDownLatch
import org.scalatest.concurrent.Eventually
import org.scalatest.BeforeAndAfterEach
import scala.collection.mutable
import org.scalatest.funsuite.AnyFunSuite

trait PartitioningServiceTestBase extends AnyFunSuite with BeforeAndAfterEach with Eventually {
  import PartitioningServiceTestBase._

  protected[this] val failingHosts = new mutable.HashSet[String]()
  protected[this] val slowHosts = new mutable.HashSet[String]()
  protected[this] var servers: Seq[(ListeningServer, InetSocketAddress, Int, Int)] = _
  protected[this] var client: Service[String, String] = _
  protected[this] var timer: MockTimer = _
  protected[this] var serverLatchOpt: Option[CountDownLatch] = _

  override def beforeEach(): Unit = {
    failingHosts.clear()
    slowHosts.clear()
    timer = new MockTimer
    serverLatchOpt = None
  }

  override def afterEach(): Unit = {
    client.close()
    servers.foreach(_._1.close())
  }

  def getPartitioningServiceModule: Stackable[ServiceFactory[String, String]]

  protected[this] def awaitResult[T](awaitable: Awaitable[T]): T = Await.result(awaitable, Timeout)

  protected[this] def createServers(
    size: Int,
    startingIndex: Int = 0
  ): Seq[(ListeningServer, InetSocketAddress, Int, Int)] = {
    def echoService(servername: String): Service[String, String] =
      Service.mk[String, String](req => {
        if (failingHosts.contains(servername)) {
          Future.exception(new RuntimeException(s"$servername failed!"))
        } else if (slowHosts.contains(servername)) {
          Future
            .sleep(12.seconds)(DefaultTimer)
            .before(
              Future.value(s"Response from $servername: after sleep")
            )
        } else {
          // sending back the hostname along with the request value, so that the caller can
          // assert that the request landed on the correct host. Also take care of multiple
          // request strings (batched request) that are delimited by RequestDelimiter
          val requests = req.split(RequestDelimiter)
          val response = requests.map(_ + EchoDelimiter + servername) // $port:$hostname
          Future.value(response.mkString(ResponseDelimiter))
        } ensure {
          serverLatchOpt match {
            case Some(latch) => latch.countDown()
            case None =>
          }
        }
      })

    // create a cluster of multiple servers, listening on unique port numbers
    startingIndex until (startingIndex + size) map { i =>
      val addr = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
      val server = StringServer.server.serve(addr, echoService(servername = s"server#$i"))
      val boundAddress = server.boundAddress.asInstanceOf[InetSocketAddress]
      val port = boundAddress.getPort
      (server, boundAddress, port, i)
    }
  }

  protected[this] def createClient(
    sr: StatsReceiver,
    dest: Name = Name.bound(servers.map(s => Address(s._2)): _*),
    ejectFailedHosts: Boolean = false
  ): Service[String, String] = {

    // create a partitioning aware finagle client by inserting the PartitioningService appropriately
    val newClientStack =
      StackClient
        .newStack[String, String]
        .insertAfter(
          BindingFactory.role,
          getPartitioningServiceModule
        )

    StringClient.client
      .withStack(newClientStack)
      .withRequestTimeout(1.second)
      .configured(Stats(sr))
      .configured(FailureAccrualFactory.Param(1, () => 10.minutes))
      .configured(partitioningParam.EjectFailedHost(ejectFailedHosts))
      .configured(ctfparam.Timer(timer))
      .newService(dest, "client")
  }
}

object PartitioningServiceTestBase {
  // Using the following delimiters to simulate batched requests. When request string contains
  // multiple delimited strings, it will be treated as a batched request, with each string segment
  // to be served with matching partition.
  val RequestDelimiter = ";"

  // When request was batched the responses will be collected and returned back after combining
  // together using the delimiter
  val ResponseDelimiter = ";"

  val EchoDelimiter = ':'

  val Timeout: Duration = 5.seconds

  def mergeStringResults(origReq: String, pr: PartitionedResults[String, String]): String = {
    // responses contain the request keys. So just concatenate. In a real implementation this will
    // typically be a key-value map.
    if (pr.failures.isEmpty) {
      pr.successes.map { case (_, v) => v }.mkString(ResponseDelimiter)
    } else if (pr.successes.isEmpty) {
      pr.failures.map { case (_, t) => t.getClass.getTypeName }.mkString(ResponseDelimiter)
    } else {
      // appending the server exceptions here to easily test partial success for batch operations
      pr.successes.map { case (_, v) => v }.mkString(ResponseDelimiter) + ResponseDelimiter +
        pr.failures.map { case (_, t) => t.getClass.getTypeName }.mkString(ResponseDelimiter)
    }
  }
}
