package com.twitter.finagle.ssl;

/**
 * Java APIs for {@link ClientAuth}.
 */
public final class ClientAuthConfig {

  private ClientAuthConfig() {
    throw new IllegalStateException();
  }

  /**
   * See {@link ClientAuth.Unspecified}
   */
  public static final ClientAuth UNSPECIFIED =
    ClientAuth.Unspecified$.MODULE$;

  /**
   * See {@link ClientAuth.Off}
   */
  public static final ClientAuth OFF =
    ClientAuth.Off$.MODULE$;

  /**
   * See {@link ClientAuth.Wanted}
   */
  public static final ClientAuth WANTED =
    ClientAuth.Wanted$.MODULE$;

  /**
   * See {@link ClientAuth.Needed}
   */
  public static final ClientAuth NEEDED =
    ClientAuth.Needed$.MODULE$;

}
