package com.twitter.finagle.mysql.unit

import com.twitter.finagle.Stack
import com.twitter.finagle.mysql.Capability
import com.twitter.finagle.mysql.HandshakeSettings
import com.twitter.finagle.mysql.MysqlCharset
import com.twitter.finagle.mysql.param.Charset
import com.twitter.finagle.mysql.param.Credentials
import com.twitter.finagle.mysql.param.Database
import com.twitter.finagle.mysql.param.Interactive
import com.twitter.finagle.mysql.param.FoundRows
import org.scalatest.funsuite.AnyFunSuite

class HandshakeSettingsTest extends AnyFunSuite {

  private val initial = Capability(
    Capability.Transactions,
    Capability.MultiResults
  )

  test("HandshakeSettings adds FoundRows by default") {
    val settings = HandshakeSettings(clientCapabilities = initial)
    assert(settings.calculatedClientCapabilities.has(Capability.FoundRows))
  }

  test("HandshakeSettings does not add FoundRows when found rows is disabled") {
    val settings = HandshakeSettings(clientCapabilities = initial, enableFoundRows = false)
    assert(!settings.calculatedClientCapabilities.has(Capability.FoundRows))
  }

  test("HandshakeSettings adds Interactive by default") {
    val settings = HandshakeSettings(clientCapabilities = initial)
    assert(settings.calculatedClientCapabilities.has(Capability.Interactive))
  }

  test("HandshakeSettings does not add Interactive when interactive is disabled") {
    val settings = HandshakeSettings(clientCapabilities = initial, interactive = false)
    assert(!settings.calculatedClientCapabilities.has(Capability.Interactive))
  }

  test("HandshakeSettings adds ConnectWithDB and SSL settings if database is defined") {
    val settings = HandshakeSettings(clientCapabilities = initial, database = Some("test"))
    assert(settings.calculatedClientCapabilities.has(Capability.ConnectWithDB))
    assert(settings.sslCalculatedClientCapabilities.has(Capability.SSL))
  }

  test("HandshakeSettings can read values from Stack params") {
    val params = Stack.Params.empty +
      Charset(MysqlCharset.Binary) +
      Credentials(Some("user123"), Some("pass123")) +
      Database(Some("test")) +
      FoundRows(false) +
      Interactive(false)
    val settings = HandshakeSettings(params)
    assert(settings.username == Some("user123"))
    assert(settings.password == Some("pass123"))
    assert(settings.database == Some("test"))
    assert(settings.charset == MysqlCharset.Binary)
    assert(!settings.enableFoundRows)
    assert(!settings.interactive)
  }
}
