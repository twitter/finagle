package com.twitter.finagle.toggle;

import scala.collection.immutable.Map;

import org.junit.Assert;
import org.junit.Test;

public class ToggleMapCompilationTest {
  @Test
  public void testMutableIsToggleMap() {
    ToggleMap toggleMap = ToggleMap.newMutable();
    Assert.assertNotNull(toggleMap);
  }

  @Test
  public void testToggleCasting() {
    ToggleMap.Mutable toggleMap = ToggleMap.newMutable();
    toggleMap.put("com.twitter.finagle.toggle.test", 1.0);
    Toggle toggle = toggleMap.get("com.twitter.finagle.toggle.test");
    Assert.assertTrue(toggle.isEnabled(5000));
  }

  @Test
  public void testRegisterLibraries() {
    Map<String, ToggleMap.Mutable> map = StandardToggleMap.registeredLibraries();
    String json = JsonToggleMap.mutableToJson(map);
    Assert.assertNotNull(map);
    Assert.assertNotNull(json);
  }
}
