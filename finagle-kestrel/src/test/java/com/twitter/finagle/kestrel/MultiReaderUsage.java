package com.twitter.finagle.kestrel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.builder.StaticCluster;
import com.twitter.finagle.kestrel.protocol.Kestrel;
import com.twitter.util.Duration;

/**
 * A compilation test for using MultiReader in Java.
 */
public class MultiReaderUsage {
  public ReadHandle make() {
    SocketAddress localhost = new InetSocketAddress("localhost", 22133);
    ArrayList<SocketAddress> clusterMembers = new ArrayList<SocketAddress>();
    clusterMembers.add(localhost);
    StaticCluster<SocketAddress> cluster =
      new StaticCluster<SocketAddress>(null);

    return
      MultiReader.newBuilder(cluster, "the-queue")
        .clientBuilder(
          ClientBuilder.get()
            .codec(new Kestrel())
            .hostConnectionLimit(1)
            .requestTimeout(Duration.fromTimeUnit(30, TimeUnit.SECONDS)))
        .build();
  }
}
