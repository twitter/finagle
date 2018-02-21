package com.twitter.finagle;


/**
 * A Java adaptation of the {@link com.twitter.finagle.Stack} companion object.
 */
public final class Stacks {
  private Stacks() { }

  /**
   * Java-friendly API for an empty parameter map.
   * @see Stack.Params$#empty()
   */
  public static final Stack.Params EMPTY_PARAMS = Stack.Params$.MODULE$.empty();
}
