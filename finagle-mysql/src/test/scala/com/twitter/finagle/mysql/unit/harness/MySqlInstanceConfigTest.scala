package com.twitter.finagle.mysql.unit.harness

import com.twitter.finagle.mysql.harness.config.InstanceConfig
import com.twitter.finagle.mysql.harness.config.MySqlVersion
import java.nio.file.Path
import java.nio.file.Paths
import org.scalatest.funsuite.AnyFunSuite

class InstanceConfigTest extends AnyFunSuite {
  val tempDir: Path = Paths.get(System.getProperty("java.io.tmpdir"))
  val testVersion = MySqlVersion(
    1,
    2,
    3,
    Map()
  )
  test("create a config with defaults values") {
    val config = InstanceConfig(
      testVersion
    )

    val expectedParameters = config.mySqlVersion.serverStartParameters.map {
      case (key, value) => s"$key=$value"
    }.toSeq

    config.startServerParameters.foreach { param =>
      assert(expectedParameters.contains(param))
    }

    assert(expectedParameters.size == config.startServerParameters.size)
    assert(config.mySqlVersion == testVersion)
    assert(
      config.extractedMySqlPath.toString.equals(s"${tempDir.resolve(".embedded_mysql/1.2.3")}"))
  }

  Seq(
    "--port",
    "--datadir"
  ).foreach { forbiddenParam =>
    test(s"should throw if $forbiddenParam is in start parameters") {
      val testVersion = MySqlVersion(
        1,
        2,
        3,
        Map(forbiddenParam -> "aValue")
      )

      val errorMessage = intercept[RuntimeException] {
        InstanceConfig(testVersion)
      }.getMessage
      assert(errorMessage.equals(s"$forbiddenParam is not allowed in startServerParameters"))
    }
  }

}
