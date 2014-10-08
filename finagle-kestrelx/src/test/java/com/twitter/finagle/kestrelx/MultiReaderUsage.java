package com.twitter.finagle.kestrelx;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import com.twitter.finagle.Addr;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.kestrelx.protocol.Kestrel;
import com.twitter.util.Duration;
import com.twitter.util.Var;
import com.twitter.util.Var$;

/**
 * A compilation test for using MultiReader in Java.
 */
public class MultiReaderUsage {
  public ReadHandle make() {
    ArrayList<SocketAddress> clusterMembers = new ArrayList<SocketAddress>();
    clusterMembers.add(new InetSocketAddress(0));
    Var<Addr> cluster = Var$.MODULE$.apply(Addr.Bound$.MODULE$.apply(clusterMembers));

    return
      MultiReader.apply(cluster, "the-queue")
        .clientBuilder(
          ClientBuilder.get()
            .codec(new Kestrel())
            .hostConnectionLimit(1)
            .requestTimeout(Duration.fromTimeUnit(30, TimeUnit.SECONDS)))
        .build();
  }

  public ReadHandle directly() {
    return ReadHandle.merged(new ArrayList<ReadHandle>().iterator());
  }
}
