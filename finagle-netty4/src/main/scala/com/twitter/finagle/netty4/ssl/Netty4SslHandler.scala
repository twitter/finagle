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
private[netty4] class Netty4SslHandler(params: Stack.Params) extends ChannelInitializer[Channel] {

  private[this] val Transport.Tls(config) = params[Transport.Tls]

  def initChannel(ch: Channel): Unit = {
    initTls(ch)
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

  private[this] def finishServerSideHandler(ch: Channel, ssl: SslHandler): Unit = {
    ssl.engine().setUseClientMode(false)

    ch.pipeline().addFirst("ssl", ssl)
  }

  private[this] def finishClientSideHandler(
    ch: Channel,
    ssl: SslHandler,
    hostnameOption: Option[String]
  ): Unit = {
    ssl.engine().setUseClientMode(true)

    // Close channel on close_notify received from a remote peer.
    ssl.sslCloseFuture().addListener(closeChannelOnCloseNotify)

    val sessionValidation = hostnameOption
      .map(SessionVerifier.hostname)
      .getOrElse(SessionVerifier.AlwaysValid)

    ch.pipeline().addFirst("ssl connect", new SslConnectHandler(ssl, sessionValidation))
    ch.pipeline().addFirst("ssl", ssl)
  }

  private[this] def clientFromJavaContext(
    ch: Channel,
    context: SSLContext,
    hostnameOption: Option[String]
  ): Unit = {
    val handler = new SslHandler(hostnameAndPort(params).fold(context.createSSLEngine()) {
      case (hostname, port) => context.createSSLEngine(hostnameOption.getOrElse(hostname), port)
    })

    finishClientSideHandler(ch, handler, hostnameOption)
  }

  private[this] def clientFromNettyContext(
    ch: Channel,
    context: SslContext,
    hostnameOption: Option[String]
  ): Unit = {
    val handler = hostnameAndPort(params).fold(context.newHandler(ch.alloc())) {
      case (hostname, port) =>
        context.newHandler(ch.alloc(), hostnameOption.getOrElse(hostname), port)
    }

    finishClientSideHandler(ch, handler, hostnameOption)
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

  private[this] def initTls(ch: Channel): Unit = config match {
    case TlsConfig.ServerCertAndKey(certPath, keyPath, caCertPath, ciphers, nextProtocols) =>
      finishServerSideHandler(ch, SslContextBuilder
        .forServer(new File(certPath), new File(keyPath))
        .trustManager(caCertPath.map(path => new File(path)).orNull)
        .ciphers(ciphers.map(s => s.split(":").toIterable.asJava).orNull)
        .applicationProtocolConfig(appProtoConfig(nextProtocols.toList.flatMap(_.split(","))))
        .build()
        .newHandler(ch.alloc())
      )

    case TlsConfig.ServerSslContext(e) =>
      finishServerSideHandler(ch, new SslHandler(e.createSSLEngine()))

    case TlsConfig.ClientHostname(hostname) =>
      clientFromNettyContext(ch, SslContextBuilder.forClient().build(), Some(hostname))

    case TlsConfig.Client =>
      clientFromNettyContext(ch, SslContextBuilder.forClient().build(), None)

    case TlsConfig.ClientNoValidation =>
      clientFromNettyContext(ch,
        SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build(),
        None
      )

    case TlsConfig.ClientSslContext(context) =>
      clientFromJavaContext(ch, context, None)

    case TlsConfig.ClientSslContextAndHostname(context, hostname) =>
      clientFromJavaContext(ch, context, Some(hostname))

    case TlsConfig.Disabled => // TLS/SSL is disabled
  }
}
