package com.twitter.finagle.ssl

@deprecated("Use a PKCS#8 formatted private key instead", "2018-01-19")
object Shell {
  def run(args: Array[String]) {
    val process = Runtime.getRuntime.exec(args)
    process.waitFor()
    require(process.exitValue == 0, "Failed to run command '%s'".format(args.mkString(" ")))
  }
}
