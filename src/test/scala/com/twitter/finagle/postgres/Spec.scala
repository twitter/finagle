package com.twitter.finagle.postgres

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, WordSpec}
import org.scalatest.junit.JUnitRunner
import org.scalatest.MustMatchers
import org.scalatest.mock.MockitoSugar

/*
 * Base class for finagle postgres tests.
 */
@RunWith(classOf[JUnitRunner])
class Spec
  extends WordSpec
  with MustMatchers
  with BeforeAndAfter
  with MockitoSugar {
}
