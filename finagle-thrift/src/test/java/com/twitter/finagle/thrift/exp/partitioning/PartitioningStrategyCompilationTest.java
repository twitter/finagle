package com.twitter.finagle.thrift.exp.partitioning;

import java.util.HashMap;
import java.util.Map;

import scala.Function1;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

import org.junit.Test;

import com.twitter.scrooge.ThriftStructIface;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.Return;
import com.twitter.test.A;

public class PartitioningStrategyCompilationTest {

  @Test
  public void testHashingStrategyCompiles() {
    Map<Object, ThriftStructIface> myMap = new HashMap<>();
    ClientHashingStrategy myHashingStrat =
      ClientHashingStrategy.create(Function.func((ThriftStructIface iface) -> myMap));
    PartitioningStrategy myStrat = myHashingStrat;
  }

  @Test
  public void testCustomStrategyCompiles() {
    Map<Integer, ThriftStructIface> myMap = new HashMap<>();
    CustomPartitioningStrategy myCustomStrat =
      ClientCustomStrategies.noResharding(Function.func((ThriftStructIface iface) -> Future.value(myMap)));
    CustomPartitioningStrategy myOtherCustomStrat =
      ClientCustomStrategies.noResharding(Function.func((ThriftStructIface iface) -> Future.value(myMap)), num -> num * 2);
    PartitioningStrategy myStrat = myCustomStrat;
  }

  @Test
  public void testRequestMergerRegistryCompiles() {
    Map<Object, ThriftStructIface> myMap = new HashMap<>();
    ClientHashingStrategy myHashingStrat =
      ClientHashingStrategy.create(Function.func((ThriftStructIface iface) -> myMap));
    PartitioningStrategy.RequestMergerRegistry myRegistry = myHashingStrat.requestMergerRegistry();
    myRegistry.addRequestMerger(new A.multiply(), reqs -> reqs.get(0));
  }

  @Test
  public void testResponseMergerRegistryCompiles() {
    Map<Object, ThriftStructIface> myMap = new HashMap<>();
    ClientHashingStrategy myHashingStrat =
      ClientHashingStrategy.create(Function.func((ThriftStructIface iface) -> myMap));
    PartitioningStrategy.ResponseMergerRegistry myRegistry = myHashingStrat.responseMergerRegistry();
    myRegistry.addResponseMerger(new A.multiply(), (reps, errs) -> new Return<>(reps.get(0)));
  }
}
