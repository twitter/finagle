package com.twitter.finagle.http.exp.routing

import scala.util.control.NonFatal

/**
 * A named parameter for matching an HTTP Request. A [[Parameter]] may be associated with
 * different discriminators that make up a route - a URI path, query params, header params, and
 * cookie params.
 *
 * @see [[https://swagger.io/specification/#parameter-object Open API Parameter Spec]] for the
 *     inspiration and expected feature set of [[Parameter Parameters]].
 * @see [[Segment.Parameterized Parameterized HTTP Path segments]].
 * @note We currently only support defining a parameter for a URI path. Query params,
 *       header params, and cookie params are not currently supported.
 */
private[http] sealed abstract class Parameter {
  def name: String

  // TODO: missing Parameter properties (https://swagger.io/specification/#parameter-object):
  //  - in (enum) {query, header, path, cookie}. for now we assume everything is for path
  //  - required (boolean)
  //  - description (string)
  //  - deprecated (boolean)

  def parse(value: String): Option[ParameterValue]

  override def toString: String = s"{$name}"
}

private[http] final case class StringParam(name: String) extends Parameter {
  override def parse(value: String): Option[StringValue] = Some(StringValue(value))
}

private[http] final case class BooleanParam(name: String) extends Parameter {

  /**
   * @note This currently matches spec laid out by OpenAPI and only supports case insensitive
   *       "true" and "false" values. We may need to expand this to support "t" and "1".
   * @see [[com.twitter.finagle.http.util.StringUtil.toBoolean]]
   */
  override def parse(value: String): Option[BooleanValue] = try {
    val b = value.toBoolean
    Some(BooleanValue(value, b))
  } catch {
    case NonFatal(_) => None
  }
}

/** See [[https://swagger.io/docs/specification/data-models/data-types/#numbers]] */
// TODO - minimum, maximum, exclusive minimum, exclusive maximum, multipleOf
private[http] sealed abstract class NumberParameter extends Parameter

private[http] sealed abstract class IntegerParameter extends NumberParameter

private[http] final case class IntParam(name: String) extends IntegerParameter {
  override def parse(value: String): Option[IntValue] = try {
    val i = value.toInt
    Some(IntValue(value, i))
  } catch {
    case NonFatal(_) => None
  }
}

private[http] final case class LongParam(name: String) extends IntegerParameter {
  override def parse(value: String): Option[LongValue] = try {
    val l = value.toLong
    Some(LongValue(value, l))
  } catch {
    case NonFatal(_) => None
  }
}

private[http] final case class FloatParam(
  name: String)
    extends NumberParameter {
  override def parse(value: String): Option[FloatValue] = try {
    val f = value.toFloat
    Some(FloatValue(value, f))
  } catch {
    case NonFatal(_) => None
  }
}

private[http] final case class DoubleParam(
  name: String)
    extends NumberParameter {
  override def parse(value: String): Option[DoubleValue] = try {
    val d = value.toDouble
    Some(DoubleValue(value, d))
  } catch {
    case NonFatal(_) => None
  }
}

// TODO - add support for exploding path styles at https://swagger.io/docs/specification/serialization/
//        which determines how to parse Array and Object parameters
// TODO - array support
// TODO - object support (https://swagger.io/specification/#schema-object)
