package com.twitter.finagle.util

import org.specs.SpecificationWithJUnit
import com.twitter.finagle.Announcer

class LoadServiceSpec extends SpecificationWithJUnit {

  "LoadService[T]()" should {

    "return a set of instances of T" in {
      LoadService[Announcer]() mustNot beEmpty
    }
  }
}
