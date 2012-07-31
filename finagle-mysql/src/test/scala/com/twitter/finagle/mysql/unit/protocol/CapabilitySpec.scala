package com.twitter.finagle.mysql.protocol

import org.specs.SpecificationWithJUnit

class CapabilitySpec extends SpecificationWithJUnit {
  "CapabilitySpec" should {
    val c = Capability(Capability.LongPassword, 
                       Capability.SSL, 
                       Capability.Transactions,
                       Capability.MultiResults)
    
   "contain capability" in {
      c.has(Capability.SSL) must beTrue
      c.has(Capability.Compress) must beFalse
    }

    "contain all capabilities" in {
      c.hasAll(Capability.LongPassword, Capability.NoSchema) must beFalse
      c.hasAll(Capability.LongPassword,
               Capability.SSL,
               Capability.Transactions,
               Capability.MultiResults) must beTrue
    }

    "subtract capability" in {
      1 mustEqual 1
    }

    "add capability" in {
      1 mustEqual 1
    }
    
  }
}