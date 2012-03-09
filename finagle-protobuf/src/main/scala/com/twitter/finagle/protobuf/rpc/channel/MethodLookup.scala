package com.twitter.finagle.protobuf.rpc.channel

/**
 * Lookup repository that maps method names to 4 bytes.
 * 
 * */
trait MethodLookup {

  	def encode(methodName: String ) : Int

	def lookup(code: Int) : String

}
