package com.twitter.finagle.service

/**
 * A classification of the result of a request/response pair.
 *
 * @see [[ResponseClassifier]]
 */
sealed abstract class ResponseClass {

  /**
   * Accommodates responses that can be partially successful.
   *
   * Determining a fractional value will typically require the server to
   * participate in this, perhaps by signaling it in a response header, or
   * within the returned response. Its meaning may also vary depending on use
   * case, for example a partial result set due to timeouts or load.
   *
   * @return a value between `0.0` and `1.0`, inclusive.
   * A value of `0.0` indicates a full failure and `1.0` indicates fully successful.
   */
  def fractionalSuccess: Double
}

object ResponseClass {

  /**
   * Represents a successful request/response.
   *
   * @param fractionalSuccess fraction of the response that was successful.
   *     Must be between `0.0` and `1.0`.
   */
  final case class Successful(override val fractionalSuccess: Double) extends ResponseClass {
    if (fractionalSuccess <= 0.0 || fractionalSuccess > 1.0) {
      throw new IllegalArgumentException(s"Fraction must be (0.0 and 1.0], was: $fractionalSuccess")
    }
  }

  /**
   * A fully successful request/response.
   *
   * @see `ResponseClasses.SUCCESS` for a Java friendly API.
   */
  val Success: Successful = Successful(1.0)

  /**
   * Represents a request/response that has failed, but the failure is
   * ignorable. Ignorables are never safe to retry.
   */
  final case object Ignorable extends ResponseClass {
    def fractionalSuccess: Double = 0.0
  }

  /**
   * An ignored request/response.
   *
   * @see `ResponseClasses.IGNORED` for a Java friendly API.
   */
  val Ignored: ResponseClass = Ignorable

  /**
   * Represents a request/response that has failed.
   *
   * @param retryable whether or not it is safe to retry.
   *
   * @see [[NonRetryableFailure]] for a failure that has completely
   *     failed and should not be retried.
   *
   * @see [[RetryableFailure]] for a failure that has completely
   *     failed and can be retried.
   */
  final case class Failed(retryable: Boolean) extends ResponseClass {
    def fractionalSuccess: Double = 0.0
  }

  /**
   * A complete failure that is not retryable.
   *
   * @see `ResponseClasses.NON_RETRYABLE_FAILURE` for a Java friendly API.
   */
  val NonRetryableFailure: Failed = Failed(retryable = false)

  /**
   * A complete failure that is retryable.
   *
   * @see `ResponseClasses.RETRYABLE_FAILURE` for a Java friendly API.
   */
  val RetryableFailure: Failed = Failed(retryable = true)

}
