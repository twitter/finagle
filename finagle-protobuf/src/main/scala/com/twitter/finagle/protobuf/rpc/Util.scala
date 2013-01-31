package com.twitter.finagle.protobuf.rpc

import java.util.List


object Util {

	def extractMethodNames(s: Service) : List[String] = {
		return Lists.transform(s.getDescriptorForType().getMethods(),
				new Function[MethodDescriptor, String]() {

					@Override
					def apply(d: MethodDescriptor) : String = {
						return d.getName()
					}
				})
	}

}
