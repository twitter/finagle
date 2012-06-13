package com.twitter.finagle.mysql.protocol

import org.specs.SpecificationWithJUnit

class CapabilitySpec extends SpecificationWithJUnit {
  "CapabilitySpec" should {
    val c = Capability(Capability.longPassword, 
                       Capability.ssl, 
                       Capability.transactions,
                       Capability.multiResults)
    
   "contain capability" in {
      c.has(Capability.ssl) must beTrue
      c.has(Capability.compress) must beFalse
    }

    "contain all capabilities" in {
      c.hasAll(Capability.longPassword, Capability.noSchema) must beFalse
      c.hasAll(Capability.longPassword,
               Capability.ssl,
               Capability.transactions,
               Capability.multiResults) must beTrue
    }
  }
}