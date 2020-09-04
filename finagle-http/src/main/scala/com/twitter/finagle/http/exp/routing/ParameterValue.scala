package com.twitter.finagle.http.exp.routing

/** A value corresponding to a [[Parameter]]. */
private[routing] sealed abstract class ParameterValue {

  /** The raw string value. */
  def value: String
  override def toString: String = value
}

private[routing] final case class StringValue(value: String) extends ParameterValue

private[routing] final case class BooleanValue(
  value: String,
  booleanValue: Boolean)
    extends ParameterValue

private[routing] final case class IntValue(value: String, intValue: Int) extends ParameterValue

private[routing] final case class LongValue(value: String, longValue: Long) extends ParameterValue

private[routing] final case class FloatValue(
  value: String,
  floatValue: Float)
    extends ParameterValue

private[routing] final case class DoubleValue(
  value: String,
  doubleValue: Double)
    extends ParameterValue
