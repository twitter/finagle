package com.twitter.finagle.exception

import com.twitter.finagle.tracing.Trace
import com.twitter.util.Time
import java.net.{SocketAddress, InetSocketAddress, InetAddress}
import org.apache.scribe.{LogEntry, ResultCode, scribe}
import scala.collection.JavaConversions._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.thrift.ThriftClientFramedCodec
import org.apache.thrift.protocol.TBinaryProtocol
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.util.GZIPStringEncoder
/**
 * A collection of methods to construct an ExceptionReceiver that logs to a ScribeHandler
 * specifically for the chickadee exception reporting service. These methods are not generic
 * enough for general use.
 */
object Reporter {
  private[Reporter] val scribeCategory = "chickadee"

  /**
   * Creates a default reporter.
   *
   * Default in this case means that the instance of ServiceException it constructs when its
   * receive method is called does not report any endpoints.
   */
  def defaultReporter(scribeHost: String, scribePort: Int, serviceName: String): Reporter = {
    new Reporter(
      new scribe.ServiceToClient(
        ClientBuilder() // these are from the b3 tracer
          .hosts(new InetSocketAddress(scribeHost, scribePort))
          .codec(ThriftClientFramedCodec())
          .hostConnectionLimit(5)
          .build(),
        new TBinaryProtocol.Factory()
      ), serviceName
    )
  }

  /**
   * Create a default client reporter.
   *
   * Default means the Reporter instance created by defaultReporter with the addition of
   * reporting the client based on the localhost address as the client endpoint.
   *
   * When partially applied, this function conforms to ClientExceptionReceiverBuilder.
   */
  def clientReporter(scribeHost: String, scribePort: Int)(sn: String) =
    defaultReporter(scribeHost, scribePort, sn).withClient()

  /**
   * Create a default source (i.e. server) reporter.
   *
   * Default means the Reporter instance created by defaultReporter with the addition of
   * reporting the source based on the SocketAddress argument.
   *
   * When partially applied, this function conforms to ServerExceptionReceiverBuilder.
   */
  def sourceReporter(scribeHost: String, scribePort: Int)(sn: String, sa: SocketAddress) =
    defaultReporter(scribeHost, scribePort, sn).withSource(sa)
}

/**
 * An implementation of ExceptionReceiver custom to the chickadee reporting service.
 *
 * Optionally logs stats to a statsReceiver if desired.
 *
 * Note that this implementation does not guarantee that a logged exception will be received
 * by the configured scribe endpoint because it just drops a failed message and does not retry.
 * This is because it is intended to log to a local (i.e. on the same machine) scribe daemon, in
 * which case there should be no network failure. If there is failure in this case, something else
 * is very wrong!
 */
sealed case class Reporter(
  client: scribe.ServiceToClient,
  serviceName: String,
  statsReceiver: StatsReceiver = NullStatsReceiver,
  private val sourceAddress: Option[String] = None,
  private val clientAddress: Option[String] = None) extends ExceptionReceiver {

  /**
   * Add a modifier to append a client address (i.e. endpoint) to a generated ServiceException.
   *
   * The endpoint string is the ip address of the host (e.g. "127.0.0.1").
   */
  def withClient(address: InetAddress = InetAddress.getLocalHost) =
    copy(clientAddress = Some(address.getHostAddress))

  /**
   * Add a modifier to append a source address (i.e. endpoint) to a generated ServiceException.
   *
   * The endpoint string is the ip of the host concatenated with the port of the socket (e.g.
   * "127.0.0.1:8080").
   */
  def withSource(address: SocketAddress) =
    address match {
      case isa: InetSocketAddress => copy(sourceAddress = Some(isa.getAddress.getHostAddress + ":" + isa.getPort))
      case _ => this // don't deal with non-InetSocketAddress types, but don't crash either
    }

  /**
   * Create a default ServiceException and fold in the modifiers (i.e. to add a source/client
   * endpoint).
   */
  def createEntry(e: Throwable) = {
    var se = new ServiceException(serviceName, e, Time.now, Trace.id.traceId.toLong)

    sourceAddress foreach { sa => se = se withSource sa }
    clientAddress foreach { ca => se = se withClient ca }

    new LogEntry(Reporter.scribeCategory, GZIPStringEncoder.encodeString(se.toJson))
  }

  /**
   * Log an exception to the specified scribe endpoint.
   *
   * See top level comment for this class for more details on performance
   * implications.
   */
  def receive(t: Throwable) {
    client.Log(createEntry(t) :: Nil) onSuccess {
      case ResultCode.OK => statsReceiver.counter("report_exception_ok").incr()
      case ResultCode.TRY_LATER => statsReceiver.counter("report_exception_try_later").incr()
    } onFailure {
      case e => statsReceiver.counter("report_exception_" + e.toString).incr()
    }
  }
}
