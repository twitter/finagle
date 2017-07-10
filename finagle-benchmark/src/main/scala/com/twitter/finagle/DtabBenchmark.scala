package com.twitter.finagle

import com.twitter.finagle.benchmark.StdBenchAnnotations
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class DtabBenchmark extends StdBenchAnnotations {

  private[this] val baseDtab =
    """
      |    /zk       => /$/com.foobar.serverset;
      |    /zk       => /$/com.foobar.fixedserverset;
      |
      |    /srv#     => /$/com.foobar.srv;
      |    /srv      => /srv#/production;
      |    /srv      => /srv#/prod;
      |
      |    /s        => /srv/local
    """.stripMargin

  @Param(Array(""))
  var dtab: String = ""

  private[this] var parsedDtab: Dtab = _

  @Setup(Level.Iteration)
  def setup(): Unit = {
    parsedDtab =
      if (dtab == "") Dtab.read(baseDtab)
      else Dtab.read(dtab)
  }

  @Benchmark
  def show(): String =
    parsedDtab.show

  @Benchmark
  def read(): Dtab =
    Dtab.read(dtab)

}
