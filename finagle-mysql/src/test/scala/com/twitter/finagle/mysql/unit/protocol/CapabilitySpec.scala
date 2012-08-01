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
      c.hasAll(
        Capability.LongPassword,
        Capability.SSL,
        Capability.Transactions,
        Capability.MultiResults
      ) must beTrue
    }

    "subtract capability" in {
      val c2 = c - Capability.SSL
      c2.has(Capability.SSL) must beFalse
    }

    "add capability" in {
      val c2 = c + Capability.LocalFiles + Capability.Compress
      c2.hasAll(
        Capability.LocalFiles, 
        Capability.Compress
      ) must beTrue
    }
  }
}