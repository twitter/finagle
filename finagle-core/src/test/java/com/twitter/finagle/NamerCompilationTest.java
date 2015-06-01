package com.twitter.finagle;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.util.Activities;
import com.twitter.util.Activity;


public class NamerCompilationTest {

  private static class IdNamer extends AbstractNamer {
    public Activity<NameTree<Name>> lookup(Path path) {
      return Activities.newValueActivity(
          (NameTree<Name>) new NameTree.Leaf<Name>(Name$.MODULE$.apply("/asdf")));
    }
  }

  @Test
  public void testIdNamerImplementation() {
    IdNamer idNamer = new IdNamer();
    Assert.assertEquals(
        new NameTree.Leaf<Name>(Name$.MODULE$.apply("/asdf")),
        idNamer.lookup(Path.read("/asdf")).sample());
  }
}
