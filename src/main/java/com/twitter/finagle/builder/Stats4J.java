package com.twitter.finagle.builder;

import com.twitter.ostrich.Stats$;

public class Stats4J {
  public static StatsReceiver Ostrich = new Ostrich(Stats$.MODULE$);
  public static StatsReceiver Logger =
    new JavaLogger(java.util.logging.Logger.getLogger("Finagle"));
}