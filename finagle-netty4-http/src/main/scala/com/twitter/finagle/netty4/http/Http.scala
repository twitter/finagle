package com.twitter.finagle.netty4.http

import com.twitter.finagle
import com.twitter.finagle.Http.nonChunkedPayloadSize
import com.twitter.finagle.Http.{param => httpparam}
import com.twitter.finagle.Stack.Params
import com.twitter.finagle._
import com.twitter.finagle.client.{Transporter, StdStackClient, StackClient}
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.http._
import com.twitter.finagle.http.filter.ClientContextFilter
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.param
import com.twitter.finagle.service.RetryBudget
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.TraceInitializerFilter
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Monitor, Duration, StorageUnit}
import io.netty.channel._
import io.netty.handler.codec.http.{HttpContentDecompressor, HttpObjectAggregator}
import io.netty.handler.codec.{http => NettyHttp}

// note: this needs to stay private since we want netty3
// vs netty4 implementations to be configured via stack params.
private[netty4] object Http extends Client[Request,Response] {

  def newService(dest: Name, label: String): Service[Request,Response] =
    client.newService(dest, label)

  def newClient(dest: Name, label: String): ServiceFactory[Request,Response] =
    client.newClient(dest, label)

  val client = Client()

  object Client {
    val stack: Stack[ServiceFactory[Request, Response]] = StackClient.newStack
      .replace(TraceInitializerFilter.role, new HttpClientTraceInitializer[Request, Response])
      .prepend(http.TlsFilter.module)
      .prepend(nonChunkedPayloadSize)
  }

  case class Client(
      stack: Stack[ServiceFactory[Request, Response]] = Client.stack,
      params: Stack.Params = StackClient.defaultParams + param.ProtocolLibrary("http"))
    extends StdStackClient[Request, Response, Client]
    with param.WithSessionPool[Client]
    with param.WithDefaultLoadBalancer[Client] {

    protected type In = Any
    protected type Out = Any

    protected def copy1(stack: Stack[ServiceFactory[Request, Response]], params: Params): Client =
      copy(stack, params)

    object HttpTransporter {
      def apply(params: Stack.Params): Transporter[In, Out] =  {
        val maxChunkSize = params[httpparam.MaxChunkSize].size
        val maxHeaderSize = params[httpparam.MaxHeaderSize].size
        val maxInitialLineSize = params[httpparam.MaxInitialLineSize].size
        val maxResponseSize = params[httpparam.MaxResponseSize].size
        val decompressionEnabled = params[httpparam.Decompression].enabled
        val streaming = params[httpparam.Streaming].enabled

        val pipelineCb = { pipeline: ChannelPipeline =>
          val codec = new NettyHttp.HttpClientCodec(
            maxInitialLineSize.inBytes.toInt,
            maxHeaderSize.inBytes.toInt,
            maxChunkSize.inBytes.toInt
          )

          pipeline.addLast("httpCodec", codec)

          if (!streaming)
            pipeline.addLast(
              "httpDechunker",
              new HttpObjectAggregator(maxResponseSize.inBytes.toInt)
            )

          if (decompressionEnabled)
            pipeline.addLast("httpDecompressor", new HttpContentDecompressor)

          ()
        }
        Netty4Transporter(pipelineCb, params)
      }
    }

    protected def newTransporter(): Transporter[In, Out] = HttpTransporter(params)

    protected def newDispatcher(transport: Transport[In, Out]): Service[Request, Response] = {
      val dispatcher = new HttpClientDispatcher(
        transport,
        params[param.Stats].statsReceiver.scope(GenSerialClientDispatcher.StatsScope)
      )
      new ClientContextFilter[Request, Response].andThen(dispatcher)
    }

    def withTls(cfg: Nothing): Client = ??? // CSL-2349

    def withTls(hostname: String): Client = ??? // CSL-2349

    def withTlsWithoutValidation: Client = ??? // CSL-2349

    def withMaxChunkSize(size: StorageUnit): Client =
      configured(httpparam.MaxChunkSize(size))

    def withMaxHeaderSize(size: StorageUnit): Client =
      configured(httpparam.MaxHeaderSize(size))

    def withMaxInitialLineSize(size: StorageUnit): Client =
      configured(httpparam.MaxInitialLineSize(size))

    def withMaxRequestSize(size: StorageUnit): Client =
      configured(httpparam.MaxRequestSize(size))

    def withMaxResponseSize(size: StorageUnit): Client =
      configured(httpparam.MaxResponseSize(size))

    def withStreaming(enabled: Boolean): Client =
      configured(httpparam.Streaming(enabled))

    def withDecompression(enabled: Boolean): Client =
      configured(httpparam.Decompression(enabled))

    def withCompressionLevel(level: Int): Client =
      configured(httpparam.CompressionLevel(level))

    // Java-friendly forwarders
    // See https://issues.scala-lang.org/browse/SI-8905
    override val withSessionPool: param.SessionPoolingParams[Client] =
      new param.SessionPoolingParams(this)
    override val withLoadBalancer: param.DefaultLoadBalancingParams[Client] =
      new param.DefaultLoadBalancingParams(this)
    override val withSessionQualifier: param.SessionQualificationParams[Client] =
      new param.SessionQualificationParams(this)
    override val withAdmissionControl: param.ClientAdmissionControlParams[Client] =
      new param.ClientAdmissionControlParams(this)
    override val withSession: param.SessionParams[Client] =
      new param.SessionParams(this)
    override val withTransport: param.ClientTransportParams[Client] =
      new param.ClientTransportParams(this)
    override def withResponseClassifier(responseClassifier: finagle.service.ResponseClassifier): Client =
      super.withResponseClassifier(responseClassifier)
    override def withRetryBudget(budget: RetryBudget): Client = super.withRetryBudget(budget)
    override def withRetryBackoff(backoff: Stream[Duration]): Client = super.withRetryBackoff(backoff)
    override def withLabel(label: String): Client = super.withLabel(label)
    override def withStatsReceiver(statsReceiver: StatsReceiver): Client =
      super.withStatsReceiver(statsReceiver)
    override def withMonitor(monitor: Monitor): Client = super.withMonitor(monitor)
    override def withTracer(tracer: tracing.Tracer): Client = super.withTracer(tracer)
    override def withExceptionStatsHandler(exceptionStatsHandler: stats.ExceptionStatsHandler): Client =
      super.withExceptionStatsHandler(exceptionStatsHandler)
    override def withRequestTimeout(timeout: Duration): Client = super.withRequestTimeout(timeout)
    override def configured[P](psp: (P, Stack.Param[P])): Client = super.configured(psp)
    override def filtered(filter: Filter[Request, Response, Request, Response]): Client =
      super.filtered(filter)
  }
}
