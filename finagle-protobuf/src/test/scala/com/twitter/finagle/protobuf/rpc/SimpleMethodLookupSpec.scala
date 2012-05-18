package com.twitter.finagle.protobuf.rpc

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

import org.junit.Assert._

import java.util.List

import org.junit.Test

import com.google.common.collect.Lists
import com.twitter.finagle.protobuf.rpc.channel.SimpleMethodLookup

object SimpleMethodLookupSpec extends SpecificationWithJUnit {

  "A method lookup" should {

        "find no collisions for eat, drink" in {
			val l = Lists.newArrayList("eat", "drink") 
			var collision = false
			try {
				new SimpleMethodLookup(l) {
					override def createEncoding(s: String) : Int  = {
						eatAndEssenCollidingHash(s);
					}
				}
			} catch { 
				case e: IllegalArgumentException => collision = true
        	}
        	collision must beFalse
        }

        "find collisions for eat, essen, drink" in {
			val l = Lists.newArrayList("eat", "essen", "drink") 
			var collision = false
			try {
				 new SimpleMethodLookup(l) {
					
					override def createEncoding(s: String) : Int = {
			 			eatAndEssenCollidingHash(s)
			 		}
		  		}
			} catch { 
				case e: IllegalArgumentException => collision = true
        	}
        	collision must beTrue
		}
		
		"find a valid method" in {
			val l = Lists.newArrayList("doThis", "doThat")
			val repo = new SimpleMethodLookup(l)
			val code = repo.encode("doThis")
			repo.lookup(code) mustEqual "doThis"
		}
        
    }
	
	def eatAndEssenCollidingHash(s: String)  : Int = {
		if ("eat".equals(s) || "essen".equals(s)) {
			return 1
		}
		return s.hashCode()
	}

  }

