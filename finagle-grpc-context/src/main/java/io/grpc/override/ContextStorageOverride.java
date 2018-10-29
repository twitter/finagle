package io.grpc.override;

import java.util.logging.Level;
import java.util.logging.Logger;

import scala.Option;
import scala.Some;

import com.twitter.util.Local;

import io.grpc.Context;
import io.grpc.Context.Storage;

/**
 * A default implementation of grpc-context's Storage that is compatible
 * with Twitter `Futures`.
 *
 * See https://grpc.io/grpc-java/javadoc/io/grpc/Context.Storage.html for
 * the reason why this class must exist at `io.grpc.override.ContextStorageOverride`.
 */
public final class ContextStorageOverride extends Storage {

  private static final Logger LOGGER = Logger.getLogger("io.grpc.override.ContextStorageOverride");

  private final Local<Context> storage;

  public ContextStorageOverride() {
    this.storage = new Local<>();
  }

  @Override
  public Context current() {
    Option<Context> ctx = storage.apply();
    if (ctx.isEmpty()) {
      return null;
    } else {
      return ctx.get();
    }
  }

  @Override
  public void detach(Context toDetach, Context toRestore) {
    if (current() != toDetach) {
      LOGGER.log(
          Level.SEVERE,
          "Context was not attached when detaching",
          new Throwable().fillInStackTrace()
      );
    }
    doAttach(toRestore);
  }

  @Override
  public Context doAttach(Context toAttach) {
    Context current = current();
    storage.set(Some.apply(toAttach));
    return current;
  }
}
