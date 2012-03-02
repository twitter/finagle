package com.twitter.finagle.protobuf.rpc;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.twitter.finagle.protobuf.rpc.channel.SimpleMethodLookup;

public class SimpleMethodLookupTest {

	@Test
	public void testConstruct() {
		List<String> l = Lists.newArrayList("eat", "drink");
		new SimpleMethodLookup(l) {

			@Override
			public int createEncoding(String s) {
				return eatAndEssenCollidingHash(s);
			}
		};

		// force a collision
		l.add("essen");
		try {
			new SimpleMethodLookup(l) {

				@Override
				public int createEncoding(String s) {
					return eatAndEssenCollidingHash(s);
				}
			};
			fail("Should fail due to collision.");
		} catch (Exception e) {
		}

	}

	protected int eatAndEssenCollidingHash(String s) {
		if ("eat".equals(s) || "essen".equals(s)) {
			return 1;
		}
		return s.hashCode();
	}

	@Test(expected = NoSuchMethodException.class)
	public void testMissingMethod() {
		List<String> l = Lists.newArrayList("doThis");
		SimpleMethodLookup repo = new SimpleMethodLookup(l);
		repo.encode("doesNotExist");
	}

	@Test(expected = NoSuchMethodException.class)
	public void testMissingLookupCode() {
		List<String> l = Lists.newArrayList();
		SimpleMethodLookup repo = new SimpleMethodLookup(l);
		repo.lookup(0);
	}

	@Test
	public void testHappyPath() {
		List<String> l = Lists.newArrayList("doThis", "doThat");
		SimpleMethodLookup repo = new SimpleMethodLookup(l);

		int code = repo.encode("doThis");
		assertEquals("doThis", repo.lookup(code));
	}

}
