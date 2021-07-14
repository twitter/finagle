package com.twitter.finagle.postgres

import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

/*
 * Base class for finagle postgres tests.
 */
class Spec
  extends AnyWordSpec
  with Matchers
  with BeforeAndAfter {
}
