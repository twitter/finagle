package com.twitter.finagle.example.java.kestrel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.util.CharsetUtil;

import com.twitter.finagle.ServiceFactory;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.kestrel.MultiReader;
import com.twitter.finagle.kestrel.ReadHandle;
import com.twitter.finagle.kestrel.ReadMessage;
import com.twitter.finagle.kestrel.java.Client;
import com.twitter.finagle.kestrel.protocol.Command;
import com.twitter.finagle.kestrel.protocol.Kestrel;
import com.twitter.finagle.kestrel.protocol.Response;
import com.twitter.finagle.service.Backoff;
import com.twitter.util.Duration;
import com.twitter.util.JavaTimer;

/**
 * Demonstrates the use of {{com.twitter.finagel.kestrel.MultiReader}}
 * in Java.
 */
public class GrabbyHands {
  public static void main(String args[]) {
    if (args.length < 2) {
      System.err.println("usage: java... QUEUE HOST1 [HOST2 HOST3...]");
      System.exit(1);
    }

    String queueName = args[0];

    JavaTimer timer = new JavaTimer();
    Callable<Iterator<Duration>> backoffs = Backoff.toJava(
      Backoff.exponential(
         // 100ms initial backoff
        Duration.fromTimeUnit(100, TimeUnit.MILLISECONDS),
        // multiplier
        2)
        // fail after 10 tries
        .take(10));

    ArrayList<ReadHandle> handles = new ArrayList<ReadHandle>();
    for (int i = 1; i < args.length; i++) {
      ServiceFactory<Command, Response> factory =
        ClientBuilder.safeBuildFactory(ClientBuilder.get()
          .codec(Kestrel.get())
          .hosts(args[i])
          .hostConnectionLimit(1));
      System.out.println("k " + args[i]);

      Client client = Client.newInstance(factory);
      handles.add(client.readReliably(queueName, timer, backoffs));
    }

    ReadHandle handle = MultiReader.apply(handles.iterator());

    while (true) {
      ReadMessage m = handle.messages().syncWait();
      System.out.println(m.bytes().toString(CharsetUtil.UTF_8));
      System.out.println(ChannelBuffers.hexDump(m.bytes()));
      m.ack().sync();
    }
  }
}
