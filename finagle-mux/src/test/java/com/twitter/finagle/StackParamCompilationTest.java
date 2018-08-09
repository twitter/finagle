package com.twitter.finagle.mux;

import scala.Option;

import org.junit.Test;

import com.twitter.finagle.Mux;
import com.twitter.finagle.Mux$param$MaxFrameSize$;
import com.twitter.finagle.Mux$param$OppTls$;
import com.twitter.finagle.mux.transport.OpportunisticTls;
import com.twitter.util.StorageUnit;

public class StackParamCompilationTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testClientParams() {
    Mux.client()
      .withStack(Mux.client().stack())
      .configured(new Mux$param$MaxFrameSize$().apply(new StorageUnit(8)).mk())
      .configured(new Mux$param$OppTls$().apply(Option.empty()).mk())
      .configured(new Mux$param$OppTls$()
          .apply(Option.apply(OpportunisticTls.Required$.MODULE$)).mk())
      .configured(new Mux$param$OppTls$()
          .apply(Option.apply(OpportunisticTls.Desired$.MODULE$)).mk())
      .configured(new Mux$param$OppTls$()
          .apply(Option.apply(OpportunisticTls.Off$.MODULE$)).mk());

  }

  @Test
  @SuppressWarnings("unchecked")
  public void testServerParams() {
    Mux.server()
      .withStack(Mux.server().stack())
      .configured(new Mux$param$MaxFrameSize$().apply(new StorageUnit(8)).mk())
      .configured(new Mux$param$OppTls$().apply(Option.empty()).mk())
      .configured(new Mux$param$OppTls$()
          .apply(Option.apply(OpportunisticTls.Required$.MODULE$)).mk())
      .configured(new Mux$param$OppTls$()
          .apply(Option.apply(OpportunisticTls.Desired$.MODULE$)).mk())
      .configured(new Mux$param$OppTls$()
          .apply(Option.apply(OpportunisticTls.Off$.MODULE$)).mk());
  }

}
