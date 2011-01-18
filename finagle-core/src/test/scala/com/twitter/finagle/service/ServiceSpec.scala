package com.twitter.finagle.service

import org.specs.Specification

import com.twitter.util.{Future, Return, Throw}

import com.twitter.finagle.{Service, Filter}

object ServiceSpec extends Specification {
  "filters" should {
    val intToString =
      new Filter[java.lang.Integer, java.lang.Integer, String, String] {
        def apply(request: java.lang.Integer, service: Service[String, String]) =
          service(request.toString) map (_.toInt)
      }

    val stringToInt = 
      new Filter[String, String, java.lang.Integer, java.lang.Integer] {
        def apply(request: String, service: Service[java.lang.Integer, java.lang.Integer]) =
          service(request.toInt) map (_.toString)
      }

    "compose" in {
      val filter: Filter[
        java.lang.Integer, java.lang.Integer,
        java.lang.Integer, java.lang.Integer] = intToString andThen stringToInt

      val result = filter(123, new Service[java.lang.Integer, java.lang.Integer] {
        def apply(request: java.lang.Integer) = Future(2 * request.intValue)
      })

      result.isReturn must beTrue
      result() must be_==(123 * 2)
    }
  }
}
