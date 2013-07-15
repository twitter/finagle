package com.twitter.finagle.exception

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.twitter.util.Time

private object JsonGenerator {
  private[this] val writer = {
    val mapper = new ObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.writer
  }

  def generate(in: Any): String = writer.writeValueAsString(in)
}

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
 * TraceId is from Zipkin.
 */
sealed private[exception] case class ServiceException private[ServiceException] (
  private val jsonValue: Map[String,Any]) {

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
  def withClient(address: String) = copy(jsonValue.updated("peer", address))

  /**
   * Include a source (i.e. server) address
   */
  def withSource(address: String) = copy(jsonValue.updated("sourceAddress", address))

  /**
   * Increment the cardinality of the ServiceException, adding the element if it does not
   * exist yet.
   */
  def incremented(cardinality: Int = 1) =
    copy(jsonValue.updated("cardinality",
      jsonValue.getOrElse("cardinality", 1).asInstanceOf[Int] + cardinality))

  /**
   * Generate a json representation of this using jerkson
   */
  def toJson: String = JsonGenerator.generate(jsonValue)
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
    "stackTrace" -> generateStackTrace(e.getStackTrace)
  )
}
