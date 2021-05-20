package com.twitter.finagle.ssl

import com.twitter.finagle.Stack
import com.twitter.finagle.param.OppTls
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.finagle.transport.Transport
import org.scalatest.funsuite.AnyFunSuite

class SnoopingLevelInterpreterTest extends AnyFunSuite {

  private[this] val optTlsOptions = Seq(
    OpportunisticTls.Off,
    OpportunisticTls.Desired,
    OpportunisticTls.Required
  )

  private[this] val clientAuthServerConfigs = Seq(
    ClientAuth.Off,
    ClientAuth.Wanted,
    ClientAuth.Needed
  ).map(c => SslServerConfiguration().copy(clientAuth = c))

  private[this] val interpreters = Seq(
    SnoopingLevelInterpreter.Param(SnoopingLevelInterpreter.Disabled),
    SnoopingLevelInterpreter.EnabledForNonNegotiatingProtocols,
    SnoopingLevelInterpreter.EnabledForNegotiatingProtocols
  )

  test("Disabled whenever the SnoopingLevelInterpreter is Disabled") {
    val base =
      Stack.Params.empty + SnoopingLevelInterpreter.Param(SnoopingLevelInterpreter.Disabled)
    for {
      c <- clientAuthServerConfigs
      l <- optTlsOptions
    } {
      val params = base + OppTls(Some(l)) + Transport.ServerSsl(Some(c))
      assert(!SnoopingLevelInterpreter.shouldEnableSnooping(params))
    }
  }

  test("Disabled for case of no SslServerConfiguration") {
    val base = Stack.Params.empty + Transport.ServerSsl(None)
    for {
      l <- optTlsOptions
      i <- interpreters
    } {
      val params = base + OppTls(Some(l)) + i
      assert(!SnoopingLevelInterpreter.shouldEnableSnooping(params))
    }
  }

  test("Non negotiating configuration requires client auth != Needed") {
    val base = Stack.Params.empty +
      Transport.ServerSsl(Some(SslServerConfiguration().copy(clientAuth = ClientAuth.Needed))) +
      SnoopingLevelInterpreter.EnabledForNonNegotiatingProtocols

    for {
      l <- optTlsOptions
    } {
      val params = base + OppTls(Some(l))
      assert(!SnoopingLevelInterpreter.shouldEnableSnooping(params))
    }
  }

  test("Non negotiating configuration only enables snooping for the Desired case") {
    val base = Stack.Params.empty +
      SnoopingLevelInterpreter.EnabledForNonNegotiatingProtocols

    for {
      c <- clientAuthServerConfigs if c.clientAuth != ClientAuth.Needed
      l <- optTlsOptions
    } {
      val params = base + Transport.ServerSsl(Some(c)) + OppTls(Some(l))
      assert(
        SnoopingLevelInterpreter.shouldEnableSnooping(params) == (l == OpportunisticTls.Desired))
    }
  }

  test("Negotiating configuration enables snooping for everything but the Off case") {
    val base = Stack.Params.empty + SnoopingLevelInterpreter.EnabledForNegotiatingProtocols

    for {
      c <- clientAuthServerConfigs
      l <- optTlsOptions
    } {
      val params = base + Transport.ServerSsl(Some(c)) + OppTls(Some(l))
      assert(SnoopingLevelInterpreter.shouldEnableSnooping(params) == (l != OpportunisticTls.Off))
    }
  }
}
