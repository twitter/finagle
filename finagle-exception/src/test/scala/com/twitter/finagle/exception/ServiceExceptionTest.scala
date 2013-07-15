package com.twitter.finagle.exception

import com.twitter.streamyj.Streamy
import com.twitter.util.{GZIPStringEncoder, Time}
import java.lang.{Throwable, StackTraceElement => javaSTE}
import java.net.{SocketAddress, InetSocketAddress, InetAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
 * An object that generates a service exception and verifies its JSON representation.
 */
private[exception] class TestServiceException(
  serviceName: String,
  exceptionMessage: String,
  time: Option[Time] = None,
  traceId: Option[Long] = None,
  clientAddress: Option[String] = None,
  sourceAddress: Option[String] = Some(InetAddress.getLocalHost.getHostName),
  cardinality: Option[Int] = None) {

  private val ste = new javaSTE("badclass", "badmethod", "badfile", 42)
  val throwable = new Throwable(exceptionMessage)
  throwable.setStackTrace(Array(ste,ste))

  private def constructServiceException = {
    var se = new ServiceException(serviceName, throwable, time.getOrElse(Time.now), traceId.getOrElse(0L))
    clientAddress foreach (ca => se = se.withClient(ca))
    sourceAddress foreach (sa => se = se.withSource(sa))
    cardinality foreach (c => se = se.incremented(c))
    se
  }

  lazy val serviceException = constructServiceException

  def verifyCompressedJSON(json: String) = {
    verifyJSON(GZIPStringEncoder.decodeString(json))
  }

  def verifyJSON(json: String) = {
    def verify[T](actual: T, expected: T, message: String, previous: Boolean = false) = {
      assert(!previous, message + ": variable already set")
      assert(actual == expected, message + ": " + actual)
      true
    }

    def verifyOption[T](received: T, expected: Option[T], fieldName: String, previous: Boolean = false, enforced: Boolean = true) = {
      if (enforced) {
        assert(expected.isDefined, "received key for non-defined field: " + fieldName)
        verify(received, expected.get, "incorrect value for " + fieldName, previous)
      } else true
    }

    def fail(badKey: String, location: String) {
      assert(false, "unknown element found in " + location + ": " + badKey)
    }

    val s = Streamy(json)

    var hasName = false
    var hasTraceId = false
    var hasTimestamp = false
    var hasExceptionContents = false
    var hasExceptionClass = false
    var hasMessage = false
    var hasStackTrace = false
    var hasClient = false
    var hasSource = false
    var hasCardinality = false

    s readObject {
      case "name" => hasName = verify(s.readString(), serviceName, "bad service name", hasName)
      case "traceId" => hasTraceId = verifyOption(s.readLong(), traceId, "bad traceId", hasTraceId, false)
      case "timestamp" => hasTimestamp = verifyOption(s.readLong(), time, "incorrect time", hasTimestamp, false)
      case "exceptionContents" => {
        assert(!hasExceptionContents, "got exception contents >1 times")
        hasExceptionContents = true

        s readObject {
          case "exceptionClass" => hasExceptionClass = verify(s.readString(), "java.lang.Throwable", "bad exception class", hasExceptionClass)
          case "message" => hasMessage = verify(s.readString(), exceptionMessage, "bad excepution message", hasMessage)
          case "stackTrace" => hasStackTrace = verify(s.readString(), ste.toString + "\n" + ste.toString, "bad stacktrace", hasStackTrace)
          case a => fail(a, "exception contents")
        }
      }
      case "peer" => hasClient = verifyOption(s.readString(), clientAddress, "peer", hasClient)
      case "sourceAddress" => hasSource = verifyOption(s.readString(), sourceAddress, "source", hasSource)
      case "cardinality" => hasCardinality = verifyOption(s.readInt(), cardinality map { _+1 }, "cardinality", hasCardinality, false)
      case a => fail(a, "service exception")
    }

    assert(hasName, "no name")
    assert(hasTraceId, "no trace id")
    assert(hasTimestamp, "no timestamp")
    assert(hasExceptionContents, "no exception contents")
    assert(hasExceptionClass, "no exception class")
    assert(hasMessage, "no message")
    assert(hasStackTrace, "no stacktrace")

    def optionalAssertDefined(o: Option[_], defined: Boolean, msg: String) {
      if (o.isDefined) assert(defined, msg + " expected but not found")
    }

    optionalAssertDefined(clientAddress, hasClient, "peer")
    optionalAssertDefined(sourceAddress, hasSource, "source")
    optionalAssertDefined(cardinality, hasCardinality, "cardinality")

    true
  }
}

class ServiceExceptionTest extends FunSuite {
  test("with no endpoint reporting serialize to JSON in the proper format") {
    val tse = new TestServiceException("service16", "my cool message", Some(Time.now), Some(124564L))
    assert(tse.verifyJSON(tse.serviceException.toJson))
  }

  test("with no endpoint reporting with >1 cardinality serialize to JSON in the proper format") {
    val tse = new TestServiceException("service16", "my cool message", Some(Time.now), Some(124564L), cardinality = Some(12))
    assert(tse.verifyJSON(tse.serviceException.toJson))
  }

  test("with client endpoint reporting serialize to JSON in the proper format") {
    val tse = new TestServiceException("service16", "my cool message", Some(Time.now), Some(124564L), clientAddress = Some("myendpoint"))
    assert(tse.verifyJSON(tse.serviceException.toJson))
  }

  test("with client endpoint reporting with >1 cardinality serialize to JSON in the proper format") {
    val tse = new TestServiceException("service16", "my cool message", Some(Time.now), Some(124564L), clientAddress = Some("myendpoint"), cardinality = Some(9))
    assert(tse.verifyJSON(tse.serviceException.toJson))
  }

  test("with source endpoint reporting serialize to JSON in the proper format") {
    val tse = new TestServiceException("service16", "my cool message", Some(Time.now), Some(124564L), sourceAddress = Some("myendpoint"))
    assert(tse.verifyJSON(tse.serviceException.toJson))
  }

  test("with source endpoint reporting with >1 cardinality serialize to JSON in the proper format") {
    val tse = new TestServiceException("service16", "my cool message", Some(Time.now), Some(124564L), sourceAddress = Some("myendpoint"), cardinality = Some(7))
    assert(tse.verifyJSON(tse.serviceException.toJson))
  }

  test("with both client and source endpoint reporting serialize to JSON in the proper format") {
    val tse = new TestServiceException("service16", "my cool message", Some(Time.now), Some(124564L), clientAddress = Some("myClientAddress"), sourceAddress = Some("mySourceAddress"))
    assert(tse.verifyJSON(tse.serviceException.toJson))
  }

  test("with both client and source endpoint reporting with >1 cardinality serialize to JSON in the proper format") {
    val tse = new TestServiceException("service16", "my cool message", Some(Time.now), Some(124564L), clientAddress = Some("myClientAddress"), sourceAddress = Some("mySourceAddress"), cardinality = Some(8))
    assert(tse.verifyJSON(tse.serviceException.toJson))
  }
}
