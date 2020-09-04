package com.twitter.finagle.http.exp.routing

private[http] sealed abstract class Segment

private[http] object Segment {

  /** [[Segment]] that represents the HTTP path separator '/'. */
  object Slash extends Segment {
    override def toString: String = "/"
  }

  /** [[Segment]] that represents a [[String]] constant path. */
  final case class Constant(value: String) extends Segment {
    override def toString: String = value
  }

  /** [[Segment]] that represents a [[Parameter parameterized]] path variable. */
  final case class Parameterized(param: Parameter) extends Segment {
    override def toString: String = param.toString
    def parse(str: String): Option[ParameterValue] = param.parse(str)
  }

}
