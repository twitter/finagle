package com.twitter.finagle.kestrel;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.twitter.finagle.Addr;
import com.twitter.finagle.Address;
import com.twitter.finagle.Addresses;
import com.twitter.finagle.Addrs;
import com.twitter.finagle.Kestrel;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.util.Duration;
import com.twitter.util.Var;
import com.twitter.util.Vars;

/**
 * A compilation test for using MultiReader in Java.
 */
public class MultiReaderCompilationTest {
  /**
   * make a MultiReader ReadHandle
   */
  @Test
  public void make() {
    ArrayList<Address> clusterMembers = new ArrayList<Address>();
    clusterMembers.add(Addresses.newInetAddress(
      new InetSocketAddress(InetAddress.getLoopbackAddress(), 0)));
    Var<Addr> cluster = Vars.newVar(Addrs.newBoundAddr(clusterMembers));
    MultiReader.apply(cluster, "the-queue")
      .clientBuilder(
        ClientBuilder.get()
          .stack(Kestrel.client())
          .hostConnectionLimit(1)
          .requestTimeout(Duration.fromTimeUnit(30, TimeUnit.SECONDS)));
  }

  @Test
  public void directly() {
    ReadHandle.merged(new ArrayList<ReadHandle>().iterator());
  }
}
