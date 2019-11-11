package com.twitter.finagle

package object service {

  /**
   * A response classifier allows developers to give Finagle the additional
   * application specific knowledge necessary in order to properly classify them.
   * Without this, Finagle can only safely make judgements about transport
   * level failures.
   *
   * As an example take an HTTP client that receives a response with a 500 status
   * code back from a server. To Finagle this is a successful request/response
   * based solely on the transport level. The application developer may want to
   * treat all 500 status codes as failures and can do so via a
   * [[com.twitter.finagle.service.ResponseClassifier]].
   *
   * It is a [[PartialFunction]] from a request/response pair to a
   * [[ResponseClass]] and as such multiple classifiers can be composed
   * together via [[PartialFunction.orElse]].
   *
   * @see `com.twitter.finagle.http.service.HttpResponseClassifier` for some
   * HTTP classification tools.
   *
   * @note Java does not understand the type alias and must be used as
   * `PartialFunction` in Java. Constructing a custom ResponseClassifier in Java
   * is achievable by implementing AbstractPartialFunction, see
   * [[com.twitter.finagle.service.ResponseClassifierCompilationTest#testCustomResponseClassifier()]]
   * for examples.
   *
   * @note Finagle's default classifier is
   * [[com.twitter.finagle.service.ResponseClassifier.Default]]
   * which is a total function fully covering the input domain.
   *
   * @note it is a good practice for users of `ResponseClassifier.apply` to
   * instead use `theClassifier.applyOrElse(input, ResponseClassifier.Default)`
   * in order to ensure that the PartialFunction will be fully covering.
   */
  type ResponseClassifier = PartialFunction[ReqRep, ResponseClass]

  //
  // An alternate approach would've been to allow application developers
  // to convert responses into a `Throw` instead of a `ResponseClass`.
  //
  // Conceptually, this fits more cleanly into the existing Finagle codebase
  // and how it handles failures. There were a couple of drawbacks with this
  // approach:
  //
  // 1. It is a strong opinion on the issue of "are errors exceptions?".
  //    This does not seem like something Finagle should be strongly
  //    opinionated about.
  //
  // 2. It makes users do "unnecessary" object modeling in that every
  //    failure needs to have some corresponding exception. For example,
  //    `HttpServerErrorException` for HTTP 500s. This is particularly
  //    uncompelling for developers that use status codes as part of their
  //    Thrift response.
  //    In the case where an Exception is returned by a Thrift service,
  //    but this is not a failure (for example, it is bad user input),
  //    this would also lead a different kind of unnecessary data modeling
  //    for creating types that are not Throws.
  //
  // 3. Converting the real response to a Throw is not an easy migration
  //    for existing users. An option to allow users to convert back to the
  //    original response outside of the Finagle stack would likely lead to
  //    confusion and the abstraction would leak out in places like
  //    `StatsFilter`.

}
