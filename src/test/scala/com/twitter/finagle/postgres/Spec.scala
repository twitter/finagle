package com.twitter.finagle.postgres

import org.scalatest.{BeforeAndAfter, WordSpec}
import org.scalatest.MustMatchers

/*
 * Base class for finagle postgres tests.
 */
class Spec
  extends WordSpec
  with MustMatchers
  with BeforeAndAfter {
}
