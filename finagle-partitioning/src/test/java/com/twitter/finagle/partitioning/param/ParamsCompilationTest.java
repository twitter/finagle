package com.twitter.finagle.partitioning.param;

import com.twitter.finagle.Stack;
import com.twitter.hashing.KeyHashers;
import org.junit.Test;
import scala.Tuple2;

public class ParamsCompilationTest {

  @Test
  public void testEjectFailedHost() {
    EjectFailedHost ejh = new EjectFailedHost(true);
    Tuple2<EjectFailedHost, Stack.Param<EjectFailedHost>> tuple = ejh.mk();
  }

  @Test
  public void testKeyHasher() {
    KeyHasher kh = new KeyHasher(KeyHashers.KETAMA());
    Tuple2<KeyHasher, Stack.Param<KeyHasher>> tuple = kh.mk();
  }

  @Test
  public void testNumReps() {
    NumReps nr = new NumReps(5);
    Tuple2<NumReps, Stack.Param<NumReps>> tuple = nr.mk();
  }
}
