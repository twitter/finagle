package io.grpc.override;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.util.Await;
import com.twitter.util.Future;

import io.grpc.Context;

import static com.twitter.util.Function.func;

public class ContextStorageOverrideTest {

  @Test
  public void testCurrent() {
    Context.Storage storage = new ContextStorageOverride();
    Assert.assertNull(storage.current());
  }

  @Test
  public void testDoAttach() {
    Context.Storage storage = new ContextStorageOverride();
    Assert.assertNull(storage.doAttach(Context.ROOT));
    Assert.assertEquals(Context.ROOT, storage.current());
  }

  @Test
  public void testDetach() {
    Context.Storage storage = new ContextStorageOverride();
    storage.detach(null, Context.ROOT);
    Assert.assertEquals(Context.ROOT, storage.current());

    storage.detach(Context.ROOT, null);
    Assert.assertNull(storage.current());
  }

  @Test
  // because `storage.doAttach` returns a value that is ignored.
  @SuppressWarnings("CheckReturnValue")
  public void testWithFutures() throws Exception {
    Context.Storage storage = new ContextStorageOverride();

    Context.Key<Integer> key = Context.key("intKey");
    storage.doAttach(Context.ROOT.withValue(key, 1));
    Future<Integer> f = Future.Done().map(func(i -> {
      int current = key.get(storage.current());
      Assert.assertEquals(1, current);
      return current + 1;
    }));

    int result = Await.result(f);
    Assert.assertEquals(2, result);
  }

}
