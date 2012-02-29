package com.twitter.finagle.protobuf.rpc

import java.util.List

import com.google.common.base.Function
import com.google.common.collect.Lists
import com.google.protobuf.Descriptors.MethodDescriptor
import com.google.protobuf.Service

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
