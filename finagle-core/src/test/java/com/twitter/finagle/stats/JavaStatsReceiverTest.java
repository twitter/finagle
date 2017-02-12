package com.twitter.finagle.stats;

import org.junit.Test;

public class JavaStatsReceiverTest {

  @Test
  public void testLoadedStatsReceiver() {
    StatsReceiver stats = LoadedStatsReceiver.self();
  }

  @Test
  public void testDefaultStatsReceiver() {
    StatsReceiver stats = DefaultStatsReceiver.self();
    StatsReceiver stats2 = DefaultStatsReceiver.get();
  }

  @Test
  public void testClientStatsReceiver() {
    StatsReceiver stats = ClientStatsReceiver.self();
  }

  @Test
  public void testServerStatsReceiver() {
    StatsReceiver stats = ServerStatsReceiver.self();
  }

  @Test
  public void testLoadedHostStatsReceiver() {
    StatsReceiver stats = LoadedHostStatsReceiver.self();
  }

}
