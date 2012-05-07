package com.twitter.finagle.ssl

object Shell {
  def run(args: Array[String]) {
    val process = Runtime.getRuntime.exec(args)
    process.waitFor()
    require(process.exitValue == 0, "Failed to run command '%s'".format(args.mkString(" ")))
  }
}
