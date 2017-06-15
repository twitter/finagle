package com.twitter.finagle.stats;

import org.junit.Test;

public class JavaStatsReceiverTest {

  @Test
  public void testLoadedStatsReceiver() {
    StatsReceiver stats = LoadedStatsReceiver.self();
  }

  @Test
  public void testDefaultStatsReceiver() {
    StatsReceiver stats2 = DefaultStatsReceiver.get();
  }

  @Test
  public void testClientStatsReceiver() {
    StatsReceiver stats = ClientStatsReceiver.get();
  }

  @Test
  public void testServerStatsReceiver() {
    StatsReceiver stats = ServerStatsReceiver.get();
  }

  @Test
  public void testLoadedHostStatsReceiver() {
    StatsReceiver stats = LoadedHostStatsReceiver.self();
  }
}
