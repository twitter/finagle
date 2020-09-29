package com.twitter.finagle.thrift.exp.partitioning;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import org.junit.Test;

import com.twitter.scrooge.ThriftStructIface;
import com.twitter.test.A;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.Return;

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
    IntFunction<List<Integer>> getLogicalPartitions = num -> Arrays.asList(num * 2);
    CustomPartitioningStrategy myOtherCustomStrat =
      ClientCustomStrategies.noResharding(Function.func((ThriftStructIface iface) -> Future.value(myMap)), getLogicalPartitions);
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
