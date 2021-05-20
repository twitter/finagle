package com.twitter.finagle.http2

import com.twitter.finagle.Stack
import org.scalatest.funsuite.AnyFunSuite

class SettingsTest extends AnyFunSuite {
  test("Settings.fromParams disables push promises by default for clients") {
    val settings = Settings.fromParams(Stack.Params.empty, isServer = false)
    assert(!settings.pushEnabled)
  }

  test("Settings.fromParams doesn't set push promises for servers") {
    val settings = Settings.fromParams(Stack.Params.empty, isServer = true)
    assert(settings.pushEnabled == null)
  }
}
