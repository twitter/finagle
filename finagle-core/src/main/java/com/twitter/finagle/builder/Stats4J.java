package com.twitter.finagle.builder;

import com.twitter.finagle.stats.JavaLoggerStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;

public class Stats4J {
  public static StatsReceiver Logger =
    new JavaLoggerStatsReceiver(java.util.logging.Logger.getLogger("Finagle"));
}