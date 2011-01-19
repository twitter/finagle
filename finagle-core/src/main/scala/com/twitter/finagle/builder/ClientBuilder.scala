package com.twitter.finagle.builder

import java.net.SocketAddress
import java.util.logging.Logger
import java.util.concurrent.Executors
import javax.net.ssl.SSLContext

import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio._
import org.jboss.netty.handler.ssl._
import org.jboss.netty.bootstrap.ClientBootstrap

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._

import com.twitter.finagle.channel._
import com.twitter.finagle.util._
import com.twitter.finagle.Service
import com.twitter.finagle.service.{RetryingService, TimeoutFilter, StatsFilter}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.loadbalancer.{
  LoadBalancerService,
  LeastQueuedStrategy, FailureAccrualStrategy}

object ClientBuilder {
  def apply() = new ClientBuilder[Any, Any]
  def get() = apply()

  lazy val defaultChannelFactory =
    new NioClientSocketChannelFactory(
      Executors.newCachedThreadPool(),
      Executors.newCachedThreadPool())
}

/**
 * A word about the default values:
 *
 *   o connectionTimeout: optimized for within a datanceter
 *   o by default, no request timeout
 */
case class ClientBuilder[Req, Rep](
  _cluster: Option[Cluster],
  _codec: Option[Codec[Req, Rep]],
  _connectionTimeout: Duration,
  _requestTimeout: Duration,
  _statsReceiver: Option[StatsReceiver],
  _loadStatistics: (Int, Duration),
  _name: Option[String],
  _hostConnectionLimit: Option[Int],
  _sendBufferSize: Option[Int],
  _recvBufferSize: Option[Int],
  _retries: Option[Int],
  _logger: Option[Logger],
  _channelFactory: Option[ChannelFactory],
  _proactivelyConnect: Option[Duration],
  _tls: Option[SSLContext],
  _startTls: Boolean)
{
  def this() = this(
    None,                                        // cluster
    None,                                        // codec
    10.milliseconds,                             // connectionTimeout
    Duration.MaxValue,                           // requestTimeout
    None,                                        // statsReceiver
    (60, 10.seconds),                            // loadStatistics
    Some("client"),                              // name
    None,                                        // hostConnectionLimit
    None,                                        // sendBufferSize
    None,                                        // recvBufferSize
    None,                                        // retries
    None,                                        // logger
    Some(ClientBuilder.defaultChannelFactory),   // channelFactory
    None,                                        // proactivelyConnect
    None,                                        // tls
    false                                        // startTls
  )

  override def toString() = {
    val options = Seq(
      "name"                -> _name,
      "cluster"             -> _cluster,
      "codec"               -> _codec,
      "connectionTimeout"   -> Some(_connectionTimeout),
      "requestTimeout"      -> Some(_requestTimeout),
      "statsReceiver"       -> _statsReceiver,
      "loadStatistics"      -> _loadStatistics,
      "hostConnectionLimit" -> Some(_hostConnectionLimit),
      "sendBufferSize"      -> _sendBufferSize,
      "recvBufferSize"      -> _recvBufferSize,
      "retries"             -> _retries,
      "logger"              -> _logger,
      "channelFactory"      -> _channelFactory,
      "proactivelyConnect"  -> _proactivelyConnect,
      "tls"                 -> _tls,
      "startTls"            -> _startTls
    )

    "ClientBuilder(%s)".format(
      options flatMap {
        case (k, Some(v)) => Some("%s=%s".format(k, v))
        case _ => None
      } mkString(", "))
  }

  def hosts(hostnamePortCombinations: String): ClientBuilder[Req, Rep] = {
    val addresses = InetSocketAddressUtil.parseHosts(
      hostnamePortCombinations)
    hosts(addresses)
  }

  def hosts(addresses: Seq[SocketAddress]): ClientBuilder[Req, Rep] = {
    val _cluster = new SocketAddressCluster(addresses)
    cluster(_cluster)
  }

  def cluster(cluster: Cluster): ClientBuilder[Req, Rep] = {
    copy(_cluster = Some(cluster))
  }

  def codec[Req1, Rep1](codec: Codec[Req1, Rep1]) =
    copy(_codec = Some(codec))

  def connectionTimeout(duration: Duration) =
    copy(_connectionTimeout = duration)

  def requestTimeout(duration: Duration) =
    copy(_requestTimeout = duration)

  def reportTo(receiver: StatsReceiver) =
    copy(_statsReceiver = Some(receiver))

  /**
   * The interval over which to aggregate load statistics.
   */
  def loadStatistics(numIntervals: Int, interval: Duration) = {
    require(numIntervals >= 1, "Must have at least 1 window to sample statistics over")

    copy(_loadStatistics = (numIntervals, interval))
  }

  def name(value: String) = copy(_name = Some(value))

  def hostConnectionLimit(value: Int) =
    copy(_hostConnectionLimit = Some(value))

  def retries(value: Int) =
    copy(_retries = Some(value))

  def sendBufferSize(value: Int) = copy(_sendBufferSize = Some(value))
  def recvBufferSize(value: Int) = copy(_recvBufferSize = Some(value))

  def channelFactory(cf: ChannelFactory) =
    copy(_channelFactory = Some(cf))

  def proactivelyConnect(duration: Duration) =
    copy(_proactivelyConnect = Some(duration))

  def tls() =
    copy(_tls = Some(Ssl.client()))

  def tlsWithoutValidation() =
    copy(_tls = Some(Ssl.clientWithoutCertificateValidation()))

  def startTls(value: Boolean) =
    copy(_startTls = true)

  def logger(logger: Logger) = copy(_logger = Some(logger))

  // ** BUILDING
  private def bootstrap(codec: Codec[Req, Rep])(host: SocketAddress) = {
    val bs = new ClientBootstrap(_channelFactory.get)
    val pf = new ChannelPipelineFactory {
      override def getPipeline = {
        val pipeline = codec.clientPipelineFactory.getPipeline
        for (ctx <- _tls) {
          val sslEngine = ctx.createSSLEngine()
          sslEngine.setUseClientMode(true)
          // sslEngine.setEnableSessionCreation(true) // XXX - need this?
          pipeline.addFirst("ssl", new SslHandler(sslEngine, _startTls))
        }

        for (logger <- _logger) {
          pipeline.addFirst("channelSnooper",
            ChannelSnooper(_name.get)(logger.info))
        }

        pipeline
      }
    }

    bs.setPipelineFactory(pf)
    bs.setOption("remoteAddress", host)
    bs.setOption("connectTimeoutMillis", _connectionTimeout.inMilliseconds)
    bs.setOption("tcpNoDelay", true)  // fin NAGLE.  get it?
    // bs.setOption("soLinger", 0)  (TODO)
    bs.setOption("reuseAddress", true)
    _sendBufferSize foreach { s => bs.setOption("sendBufferSize", s) }
    _recvBufferSize foreach { s => bs.setOption("receiveBufferSize", s) }
    bs
  }

  private def pool(limit: Option[Int])(bootstrap: ClientBootstrap) =
    limit match {
      case Some(limit) =>
        throw new IllegalArgumentException("pool limits not yet supported")
      case None =>
        new PoolingService[Req, Rep](bootstrap)
    }

  private def makeBroker(codec: Codec[Req, Rep]) =
    pool(_hostConnectionLimit) _ compose bootstrap(codec)

  def build(): Service[Req, Rep] = {
    if (!_cluster.isDefined)
      throw new IncompleteSpecification("No hosts were specified")
    if (!_codec.isDefined)
      throw new IncompleteSpecification("No codec was specified")

    val cluster = _cluster.get
    val codec = _codec.get

    val brokers = cluster mkServices { host =>
      // TODO: stats export [observers], internal LB stats.
      var broker: Service[Req, Rep] = makeBroker(codec)(host)
      if (_requestTimeout < Duration.MaxValue) {
        val timeoutFilter = new TimeoutFilter[Req, Rep](Timer.default, _requestTimeout)
        broker = timeoutFilter andThen broker
      }

      if (_statsReceiver.isDefined) {
        val statsFilter = new StatsFilter[Req, Rep](
          _statsReceiver.get.scope("host" -> host.toString))
        broker = statsFilter andThen broker
      }

      broker
    }

    // TODO: enable passing in a strategy via the builder.
    val loadBalancerStrategy = {
      val leastQueuedStrategy = new LeastQueuedStrategy[Req, Rep]
      new FailureAccrualStrategy(leastQueuedStrategy, 3, 10.seconds)
    }

    val loadBalanced = new LoadBalancerService(brokers, loadBalancerStrategy)
    if (_retries.isDefined) {
      RetryingService.tries[Req, Rep](_retries.get) andThen loadBalanced
    } else {
      loadBalanced
    }
  }
}
