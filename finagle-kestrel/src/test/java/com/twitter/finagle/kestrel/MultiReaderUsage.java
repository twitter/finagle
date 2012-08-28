package com.twitter.finagle.kestrel;

import com.twitter.util.Duration;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.builder.StaticCluster;
import com.twitter.finagle.kestrel.protocol.Kestrel;
import java.util.concurrent.TimeUnit;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import scala.collection.JavaConversions.*;

/**
 * A compilation test for using MultiReader in Java.
 */
public class MultiReaderUsage {
  public ReadHandle make() {
    SocketAddress localhost = new InetSocketAddress("localhost", 22133);
    ArrayList<SocketAddress> clusterMembers = new ArrayList<SocketAddress>();
    clusterMembers.add(localhost);
    StaticCluster<SocketAddress> cluster =
      new StaticCluster<SocketAddress>(new JListWrapper(clusterMembers));

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
