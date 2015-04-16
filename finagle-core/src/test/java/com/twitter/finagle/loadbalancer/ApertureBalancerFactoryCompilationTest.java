/* Copyright 2015 Twitter, Inc. */
package com.twitter.finagle.loadbalancer;

import scala.Tuple2;
import scala.collection.immutable.HashSet;
import scala.collection.immutable.Set;

import org.junit.Test;

import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.finagle.ServiceFactory;
import com.twitter.finagle.stats.NullStatsReceiver$;
import com.twitter.util.Activity;

public class ApertureBalancerFactoryCompilationTest {

  @Test
  public void testGet() {
    Set<Tuple2<ServiceFactory<String, String>, Object>> set =
        new HashSet<Tuple2<ServiceFactory<String, String>, Object>>();
    ApertureBalancerFactory.get().newWeightedLoadBalancer(
        Activity.value(set),
        NullStatsReceiver$.MODULE$,
        new NoBrokersAvailableException("test"));
  }

}
