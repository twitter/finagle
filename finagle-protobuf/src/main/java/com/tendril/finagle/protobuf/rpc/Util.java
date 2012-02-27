package com.tendril.finagle.protobuf.rpc;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Service;

public class Util {

	public static List<String> extractMethodNames(Service s) {
		return Lists.transform(s.getDescriptorForType().getMethods(),
				new Function<MethodDescriptor, String>() {

					@Override
					public String apply(MethodDescriptor d) {
						return d.getName();
					}
				});
	}

}
