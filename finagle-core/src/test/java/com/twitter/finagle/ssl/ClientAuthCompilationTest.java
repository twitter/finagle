package com.twitter.finagle.ssl;

import org.junit.Test;

public class ClientAuthCompilationTest {

  @Test
  public void testUnspecified() {
    ClientAuth clientAuth = ClientAuthConfig.UNSPECIFIED;
  }

  @Test
  public void testOff() {
    ClientAuth clientAuth = ClientAuthConfig.OFF;
  }

  @Test
  public void testWanted() {
    ClientAuth clientAuth = ClientAuthConfig.WANTED;
  }

  @Test
  public void testNeeded() {
    ClientAuth clientAuth = ClientAuthConfig.NEEDED;
  }

}
