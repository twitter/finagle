package com.twitter.finagle.java;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.channel.*;
import org.jboss.netty.bootstrap.*;
import org.jboss.netty.channel.socket.nio.*;
import org.jboss.netty.handler.codec.http.*;

import com.twitter.finagle.channel.*;
import com.twitter.finagle.util.*;

class NoopBroker extends SocketAddress implements Broker {
  Broker underlying_;

  NoopBroker(Broker underlying) {
    underlying_ = underlying;
  }

  public ReplyFuture dispatch(MessageEvent e) {
    return underlying_.dispatch(e);
  }
}

class InterfaceTest {
  public static void main(String args[]) {
    NioClientSocketChannelFactory channelFactory =
        new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool());

    final ClientBootstrap clientBootstrap = new ClientBootstrap(channelFactory);
    clientBootstrap.setPipelineFactory(
        new ChannelPipelineFactory() {
          @Override
          public ChannelPipeline getPipeline() {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("httpCodec", new HttpClientCodec());
            return pipeline;
          }
        }
    );
    clientBootstrap.setOption("remoteAddress", new InetSocketAddress("twitter.com", 80));

    ChannelPool pool = new ChannelPool(clientBootstrap);
    Broker poolingBroker = new PoolingBroker(pool);

    BrokeredChannelFactory brokeredChannelFactory = new BrokeredChannelFactory();
    final ClientBootstrap brokeredBootstrap = new ClientBootstrap(brokeredChannelFactory);


    brokeredBootstrap.setPipelineFactory(
        new ChannelPipelineFactory() {
          @Override
          public ChannelPipeline getPipeline() {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast(
                "handler",
                new SimpleChannelUpstreamHandler() {
                  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
                    System.out.println("received " + e.getMessage());
                  }
                }
            );
            return pipeline;
          }
        }
    );
    brokeredBootstrap.setOption("remoteAddress", poolingBroker);

    ChannelFuture future = brokeredBootstrap.connect();
    future.awaitUninterruptibly();
    Channel channel = future.getChannel();

    // Ok, now write the HTTP request.
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    Channels.write(channel, httpRequest);

    channel.getCloseFuture().awaitUninterruptibly();
  }
}
