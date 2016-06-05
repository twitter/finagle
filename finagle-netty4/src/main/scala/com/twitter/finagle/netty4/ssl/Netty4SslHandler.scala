package com.twitter.finagle.netty4.ssl

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.ssl.SessionVerifier
import com.twitter.finagle.transport.{TlsConfig, Transport}
import com.twitter.finagle.{Address, Stack}
import io.netty.handler.ssl.ApplicationProtocolConfig.{
  SelectedListenerFailureBehavior, SelectorFailureBehavior, Protocol
}
import io.netty.channel._
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import io.netty.handler.ssl.{ApplicationProtocolConfig, SslContext, SslContextBuilder, SslHandler}
import io.netty.util.concurrent.{Future => NettyFuture, GenericFutureListener}
import io.netty.util.internal.OneTimeTask
import java.io.File
import javax.net.ssl.SSLContext
import scala.collection.JavaConverters._

/**
 * An internal channel handler that takes [[Stack.Params]] and upgrades the pipeline with missing
 * TLS/SSL pieces required for either server- or client-side transport encryption.
 *
 * No matter if the underlying pipeline has been modified or not (or exception was thrown), this
 * handler removes itself from the pipeline on `handlerAdded`.
 */
private[netty4] class Netty4SslHandler(params: Stack.Params) extends ChannelHandlerAdapter {

  private[this] val Transport.Tls(config) = params[Transport.Tls]

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    try initTls(ctx) finally ctx.pipeline().remove(this)
  }

  // The reason we can't close the channel immediately is because we're in process of decoding an
  // inbound message that is represented by a bunch of TLS records. We need to finish decoding
  // and send that message up to the pipeline before closing the channel. This is why we queue the
  // close event.
  //
  // See CSL-1610 (internal ticket) for more details.
  private[this] val closeChannelOnCloseNotify =
    new GenericFutureListener[NettyFuture[Channel]] {
      def operationComplete(f: NettyFuture[Channel]): Unit = {
        val channel = f.getNow
        channel.eventLoop().execute(new OneTimeTask {
          def run(): Unit = channel.close()
        })
      }
    }

  private[this] def hostnameAndPort(params: Stack.Params): Option[(String, Int)] = {
    val Transporter.EndpointAddr(addr) = params[Transporter.EndpointAddr]

    addr match {
      case Address.Inet(isa, _) => Some(isa.getHostName -> isa.getPort)
      case _ => None
    }
  }

  private[this] def finishServerSideHandler(ctx: ChannelHandlerContext, ssl: SslHandler): Unit = {
    ssl.engine().setUseClientMode(false)
    // `true` is default for most of the engine implementations so we do that just in case.
    ssl.engine().setEnableSessionCreation(true)

    ctx.pipeline().addFirst("ssl", ssl)
  }

  private[this] def finishClientSideHandler(
    ctx: ChannelHandlerContext,
    ssl: SslHandler,
    hostnameOption: Option[String]
  ): Unit = {
    ssl.engine().setUseClientMode(true)
    // `true` is default for most of the engine implementations so we do that just in case.
    ssl.engine().setEnableSessionCreation(true)

    // Close channel on close_notify received from a remote peer.
    ssl.sslCloseFuture().addListener(closeChannelOnCloseNotify)

    val sessionValidation = hostnameOption
      .map(SessionVerifier.hostname)
      .getOrElse(SessionVerifier.AlwaysValid)

    ctx.pipeline().addFirst("ssl connect", new SslConnectHandler(ssl, sessionValidation))
    ctx.pipeline().addFirst("ssl", ssl)
  }

  private[this] def clientFromJavaContext(
    ctx: ChannelHandlerContext,
    context: SSLContext,
    hostnameOption: Option[String]
  ): Unit = {
    val handler = new SslHandler(hostnameAndPort(params).fold(context.createSSLEngine()) {
      case (hostname, port) => context.createSSLEngine(hostnameOption.getOrElse(hostname), port)
    })

    finishClientSideHandler(ctx, handler, hostnameOption)
  }

  private[this] def clientFromNettyContext(
    ctx: ChannelHandlerContext,
    context: SslContext,
    hostnameOption: Option[String]
  ): Unit = {
    val handler = hostnameAndPort(params).fold(context.newHandler(ctx.alloc())) {
      case (hostname, port) =>
        context.newHandler(ctx.alloc(), hostnameOption.getOrElse(hostname), port)
    }

    finishClientSideHandler(ctx, handler, hostnameOption)
  }

  private[this] def appProtoConfig(nextProtocols: Iterable[String]): ApplicationProtocolConfig =
    if (nextProtocols.isEmpty) ApplicationProtocolConfig.DISABLED
    else {
      new ApplicationProtocolConfig(
        Protocol.NPN_AND_ALPN,
        // NO_ADVERTISE and ACCEPT are the only modes supported by both OpenSSL and JDK SSL.
        SelectorFailureBehavior.NO_ADVERTISE,
        SelectedListenerFailureBehavior.ACCEPT,
        nextProtocols.asJava
      )
    }

  private[this] def initTls(ctx: ChannelHandlerContext): Unit = config match {
    case TlsConfig.ServerCertAndKey(certPath, keyPath, caCertPath, ciphers, nextProtocols) =>
      finishServerSideHandler(ctx, SslContextBuilder
        .forServer(new File(certPath), new File(keyPath))
        .trustManager(caCertPath.map(path => new File(path)).orNull)
        .ciphers(ciphers.map(s => s.split(":").toIterable.asJava).orNull)
        .applicationProtocolConfig(appProtoConfig(nextProtocols.toList.flatMap(_.split(","))))
        .build()
        .newHandler(ctx.alloc())
      )

    case TlsConfig.ServerSslContext(e) =>
      finishServerSideHandler(ctx, new SslHandler(e.createSSLEngine()))

    case TlsConfig.ClientHostname(hostname) =>
      clientFromNettyContext(ctx, SslContextBuilder.forClient().build(), Some(hostname))

    case TlsConfig.Client =>
      clientFromNettyContext(ctx, SslContextBuilder.forClient().build(), None)

    case TlsConfig.ClientNoValidation =>
      clientFromNettyContext(ctx,
        SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build(),
        None
      )

    case TlsConfig.ClientSslContext(context) =>
      clientFromJavaContext(ctx, context, None)

    case TlsConfig.ClientSslContextAndHostname(context, hostname) =>
      clientFromJavaContext(ctx, context, Some(hostname))

    case TlsConfig.Disabled => // TLS/SSL is disabled
  }
}