package com.twitter.finagle.exception

import org.specs.Specification
import com.twitter.streamyj.Streamy
import java.lang.{Throwable, StackTraceElement => javaSTE}
import com.twitter.util.{GZIPStringEncoder, Time}

/**
 * An object that generates a service exception and verifies its JSON representation.
 */
private[exception] class TestServiceException(
  serviceName: String,
  exceptionMessage: String,
  time: Option[Time] = None,
  traceId: Option[Long] = None,
  clientAddress: Option[String] = None,
  sourceAddress: Option[String] = None) {

  private val ste = new javaSTE("badclass", "badmethod", "badfile", 42)
  val throwable = new Throwable(exceptionMessage)
  throwable.setStackTrace(Array(ste,ste))

  private def constructServiceException = {
    var se = new ServiceException(serviceName, throwable, time.getOrElse(Time.now), traceId.getOrElse(0L))
    clientAddress foreach (ca => se = se.withClient(ca))
    sourceAddress foreach (sa => se = se.withSource(sa))
    se
  }

  lazy val serviceException = constructServiceException

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
          case "stacktrace" => hasStackTrace = verify(GZIPStringEncoder.decodeString(s.readString()), ste.toString + "\n" + ste.toString, "bad stacktrace", hasStackTrace)
          case a => fail(a, "exception contents")
        }
      }
      case "client" => hasClient = verifyOption(s.readString(), clientAddress, "client", hasClient)
      case "source" => hasSource = verifyOption(s.readString(), sourceAddress, "source", hasSource)
      case a => fail(a, "service exception")
    }

    assert(hasName, "no name")
    assert(hasTraceId, "no trace id")
    assert(hasTimestamp, "no timestamp")
    assert(hasExceptionContents, "no exception contents")
    assert(hasExceptionClass, "no exception class")
    assert(hasMessage, "no message")
    assert(hasStackTrace, "no stacktrace")

    true
  }
}

object ServiceExceptionSpec extends Specification {
  "A ServiceException with no endpoint reporting" should {

    "serialize to JSON in the proper format" in {
      val tse = new TestServiceException("service16", "my cool message", Some(Time.now), Some(124564L))
      tse.verifyJSON(tse.serviceException.toJson) mustBe true
    }
  }

  "A ServiceException with client endpoint reporting" should {
    "serialize to JSON in the proper format" in {
      val tse = new TestServiceException("service16", "my cool message", Some(Time.now), Some(124564L), clientAddress = Some("myendpoint"))
      tse.verifyJSON(tse.serviceException.toJson) mustBe true
    }
  }

  "A ServiceException with source endpoint reporting" should {
    "serialize to JSON in the proper format" in {
      val tse = new TestServiceException("service16", "my cool message", Some(Time.now), Some(124564L), sourceAddress = Some("myendpoint"))
      tse.verifyJSON(tse.serviceException.toJson) mustBe true
    }
  }

  "A ServiceException with both client and source endpoint reporting" should {
    "serialize to JSON in the proper format" in {
      val tse = new TestServiceException("service16", "my cool message", Some(Time.now), Some(124564L), clientAddress = Some("myClientAddress"), sourceAddress = Some("mySourceAddress"))
      tse.verifyJSON(tse.serviceException.toJson) mustBe true
    }
  }
}
