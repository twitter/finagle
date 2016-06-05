package com.twitter.finagle.context;

import scala.runtime.BoxedUnit;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.util.Function0;

public class LocalContextCompilationTest {

  @Test
  public void testLocalContextBinding() {
    final LocalContext ctx = new com.twitter.finagle.context.LocalContext();
    final LocalContext.Key<String> boundKey = ctx.<String>newKey();
    final LocalContext.Key<String> unboundKey = ctx.<String>newKey();

    ctx.let(boundKey, "bound value",
        new Function0<BoxedUnit>() {
          @Override
          public BoxedUnit apply() {
            Assert.assertEquals(ctx.get(boundKey), scala.Some.apply("bound value"));
            Assert.assertEquals(ctx.get(unboundKey), scala.None$.MODULE$);
            return BoxedUnit.UNIT;
          }
        });
  }

  @Test
  public void testContextShadowing() {
    final LocalContext ctx = new LocalContext();
    final LocalContext.Key<String> key = ctx.<String>newKey();
    final LocalContext.Key<String> key2 = ctx.<String>newKey();


    ctx.let(key2, "key2 value", new Function0<BoxedUnit>() {
      @Override
      public BoxedUnit apply() {
        ctx.let(key, "key1 initial value",
            new Function0<BoxedUnit>() {
              @Override
              public BoxedUnit apply() {
                Assert.assertEquals(ctx.get(key), scala.Some.apply("key1 initial value"));
                ctx.let(key, "key1 shadowing value", new Function0<BoxedUnit>() {
                  @Override
                  public BoxedUnit apply() {
                    Assert.assertEquals(ctx.get(key2), scala.Some.apply("key2 value"));
                    Assert.assertEquals(ctx.get(key), scala.Some.apply("key1 shadowing value"));
                    return BoxedUnit.UNIT;
                  }
                });
                return BoxedUnit.UNIT;
              }
            });
        return BoxedUnit.UNIT;
      }
    });
  }
}
