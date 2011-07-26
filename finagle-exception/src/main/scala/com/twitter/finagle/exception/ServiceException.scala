package com.twitter.finagle.exception

import com.codahale.jerkson.Json.generate
import com.twitter.util.Time
import scala.collection.mutable.StringBuilder

/**
 * Model classes for exception serialization to JSON
 *
 * Each of these classes define an acyclic class hierarchy for
 * an exception model for serializing information to the exception
 * service.
 *
 * These classes are private to the exception package because they must adhere to the chickadee
 * specification.
 *
 * TraceId is from BigBrotherBird.
 */
sealed private[exception] case class ServiceException private[ServiceException] (jsonValue: Map[String,Any]) {

  /**
   * Create a map with all of the elements required by a chickadee service.
   *
   * This is the only way to make new ServiceException so it can be guaranteed that none will
   * have fewer than the minimum elements per the chickadee specification.
   */
  def this(name: String, e: Throwable, timestamp: Time, traceId: Long) = this(
    Map(
      "name" -> name,
      "exceptionContents" -> ExceptionContents(e).jsonValue,
      "timestamp" -> timestamp.inMillis,
      "traceId" -> traceId
    )
  )

  /**
   * Include a client address
   */
  def withClient(address: String) = copy(jsonValue + ("client" -> address))

  /**
   * Include a source (i.e. server) address
   */
  def withSource(address: String) = copy(jsonValue + ("source" -> address))

  /**
   * Generate a json representation of this using jerkson
   */
  def toJson: String = generate(jsonValue)
}

/**
 * The contents of a java throwable, in the model format for json serialization
 */
sealed private[exception] case class ExceptionContents(e: Throwable) {
  /**
   * Generate the stack trace as a string of the concatenated java.lang.StackTraceElements
   * delimited by newline characters. The elements are in order with the stacktrace.
   */
  private def generateStackTrace(st: Array[java.lang.StackTraceElement]): String =
    st mkString "\n"

  val jsonValue = Map(
    "exceptionClass" -> e.getClass.getName,
    "message" -> e.getMessage,
    "stacktrace" -> generateStackTrace(e.getStackTrace)
  )
}
