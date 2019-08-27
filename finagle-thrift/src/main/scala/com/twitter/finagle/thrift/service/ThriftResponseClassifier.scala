package com.twitter.finagle.thrift.service

import com.twitter.finagle.service._
import com.twitter.finagle.thrift.{ClientDeserializeCtx, ServerToReqRep}
import com.twitter.util.{Return, Throw, Try}
import scala.util.control.NonFatal

/**
 * `ResponseClassifiers` for use with `finagle-thrift`
 * request/responses.
 *
 * Thrift (and ThriftMux) services are a bit unusual in that
 * there is only a single `Service` from `Array[Byte]`
 * to `Array[Byte]` for all the methods of an IDL's service.
 *
 * Thrift classifiers should be written in terms
 * of the Scrooge generated request `$Service.$Method.Args` type and the
 * method's response type. This is because there is support in Scrooge
 * and `Thrift.newService/newClient`
 * to deserialize the responses into the expected application types
 * so that classifiers can be written in a normal way.
 *
 * Given an idl:
 * <pre>
 * exception NotFoundException { 1: string reason }
 * exception RateLimitedException { 1: string reason }
 *
 * service SocialGraph {
 *   i32 follow(1: i64 follower, 2: i64 followee) throws (
 *     1: NotFoundException ex, 2: RateLimitedException ex
 *   )
 * }
 * </pre>
 *
 * One possible custom classifier would be:
 * {{{
 * val classifier: ResponseClassifier = {
 *   case ReqRep(_, Throw(_: RateLimitedException)) => RetryableFailure
 *   case ReqRep(_, Throw(_: NotFoundException)) => NonRetryableFailure
 *   case ReqRep(_, Return(x: Int)) if x == 0 => NonRetryableFailure
 *   case ReqRep(SocialGraph.Follow.Args(a, b), _) if a <= 0 || b <= 0 => NonRetryableFailure // avoid this style!
 * }
 * }}}
 *
 * Often times, a good default classifier is
 * [[ThriftResponseClassifier.ThriftExceptionsAsFailures]] which treats
 * any Thrift response that deserializes into an Exception as
 * a non-retryable failure.
 *
 * @see The user guide for more information on Response Classification of Thrift and ThriftMux
 *      [[https://twitter.github.io/finagle/guide/Clients.html#response-classification Clients]]  and
 *      [[https://twitter.github.io/finagle/guide/Servers.html#response-classification Servers]].
 */
object ThriftResponseClassifier {

  /**
   * Categorizes responses where the '''deserialized''' response is a
   * Thrift Exception as a `ResponseClass.NonRetryableFailure`.
   */
  val ThriftExceptionsAsFailures: ResponseClassifier =
    ResponseClassifier.named("ThriftExceptionsAsFailures") {
      case ReqRep(_, Throw(_)) => ResponseClass.NonRetryableFailure
    }

  /**
   * Thrift responses need to be in deserialized from in order to do
   * meaningful classification.
   *
   * The returned classifier use [[com.twitter.finagle.thrift.ClientDeserializeCtx]] for clients
   * get responses in deserialized form. This context local is expected to be configured
   * by Scrooge or some other means.
   *
   * The returned classifier's [[PartialFunction.isDefinedAt]] will return
   * `false` if there is no context local available so this is
   * safe to use with code that does not set context locals mentioned above.
   *
   * @note any exceptions thrown during deserialization will be ignored
   *       if `apply` is guarded properly with `isDefinedAt`.
   *
   * @see [[com.twitter.finagle.Thrift.newClient newClient and newService]]
   *      which will automatically apply these transformations to a [[ResponseClassifier]].
   */
  private[finagle] def usingDeserializeCtx(classifier: ResponseClassifier): ResponseClassifier =
    new ResponseClassifier {

      override def toString: String =
        s"Thrift.usingDeserializeCtx(${classifier.toString})"

      def isDefinedAt(reqRep: ReqRep): Boolean = {
        val deserCtx = ClientDeserializeCtx.get
        if (deserCtx eq ClientDeserializeCtx.nullDeserializeCtx)
          return false

        reqRep.response match {
          // we use the deserializer only if its a thrift response
          case Return(bytes: Array[Byte]) =>
            try classifier.isDefinedAt(deserCtx(bytes))
            catch {
              case _: Throwable => false
            }
          // otherwise, we see if the classifier can handle this as is
          case _ =>
            try classifier.isDefinedAt(reqRep)
            catch {
              case _: Throwable => false
            }
        }
      }

      def apply(reqRep: ReqRep): ResponseClass =
        reqRep.response match {
          // we use the deserializer only if its a thrift response
          case Return(bytes: Array[Byte]) =>
            val deserCtx = ClientDeserializeCtx.get
            if (deserCtx eq ClientDeserializeCtx.nullDeserializeCtx)
              throw new MatchError("No DeserializeCtx found")
            try {
              classifier(deserCtx(bytes))
            } catch {
              case NonFatal(e) => throw new MatchError(e)
            }
          // otherwise, we see if the classifier can handle this as is
          case _ =>
            try classifier(reqRep)
            catch {
              case NonFatal(e) => throw new MatchError(e)
            }
        }
    }

  /**
   * A [[ResponseClassifier]] that uses a Context local
   * [[com.twitter.finagle.thrift.ClientDeserializeCtx]] for clients to get responses in
   * deserialized form, while using [[ResponseClassifier.Default]] for the actual
   * response classification.
   *
   * Used when a user does not wire up a [[ResponseClassifier]]
   * to a [[com.twitter.finagle.Thrift.Client Thrift client]].
   */
  private[finagle] val DeserializeCtxOnly: ResponseClassifier =
    new ResponseClassifier {
      override def toString: String = "DefaultThriftResponseClassifier"

      // we want the side-effect of deserialization if it has not
      // yet been done
      private[this] def deserializeIfPossible(rep: Try[Any]): Unit = {
        rep match {
          case Return(bytes: Array[Byte]) =>
            val deserCtx = ClientDeserializeCtx.get
            if (deserCtx ne ClientDeserializeCtx.nullDeserializeCtx) {
              try {
                deserCtx(bytes).response
              } catch {
                case _: Throwable =>
              }
            }
          case _ =>
        }
      }

      def isDefinedAt(reqRep: ReqRep): Boolean = {
        deserializeIfPossible(reqRep.response)
        ResponseClassifier.Default.isDefinedAt(reqRep)
      }

      def apply(reqRep: ReqRep): ResponseClass = {
        deserializeIfPossible(reqRep.response)
        ResponseClassifier.Default(reqRep)
      }
    }

  /**
   * Thrift responses need to be in deserialized form in order to do
   * meaningful classification.
   *
   * The returned classifier use [[com.twitter.finagle.thrift.ServerToReqRep]] for servers
   * to get responses in deserialized form. This context local is expected to be configured
   * by Scrooge or some other means.
   *
   * The returned classifier's [[PartialFunction.isDefinedAt]] will return
   * `false` if there is no context local available so this is
   * safe to use with code that does not set context locals mentioned above.
   *
   * @note any exceptions thrown during deserialization will be ignored
   *       if `apply` is guarded properly with `isDefinedAt`.
   *
   * @see [[com.twitter.finagle.Thrift.serve]] which will automatically apply these
   *     transformations to a [[ResponseClassifier]].
   */
  private[finagle] def usingReqRepCtx(classifier: ResponseClassifier): ResponseClassifier =
    new ResponseClassifier {

      override def toString: String =
        s"Thrift.usingReqRepCtx(${classifier.toString})"

      def isDefinedAt(reqRep: ReqRep): Boolean = {
        val deserCtx = ServerToReqRep.get
        if (deserCtx eq ServerToReqRep.nullDeserializeCtx)
          return false

        reqRep.response match {
          // we use the deserializer only if its a thrift response
          case Return(bytes: Array[Byte]) =>
            try classifier.isDefinedAt(deserCtx(bytes))
            catch {
              case _: Throwable => false
            }
          // otherwise, we see if the classifier can handle this as is
          case _ =>
            try classifier.isDefinedAt(reqRep)
            catch {
              case _: Throwable => false
            }
        }
      }

      def apply(reqRep: ReqRep): ResponseClass =
        reqRep.response match {
          // we use the deserializer only if its a thrift response
          case Return(bytes: Array[Byte]) =>
            val deserCtx = ServerToReqRep.get
            if (deserCtx eq ServerToReqRep.nullDeserializeCtx)
              throw new MatchError("No DeserializeCtx found")
            try {
              classifier(deserCtx(bytes))
            } catch {
              case NonFatal(e) => throw new MatchError(e)
            }
          // otherwise, we see if the classifier can handle this as is
          case _ =>
            try classifier(reqRep)
            catch {
              case NonFatal(e) => throw new MatchError(e)
            }
        }
    }

  /**
   * A [[ResponseClassifier]] that uses a Context local
   * [[com.twitter.finagle.thrift.ServerToReqRep]] for servers to get responses in
   * deserialized form, while using [[ResponseClassifier.Default]] for the actual
   * response classification.
   *
   * Used when a user does not wire up a [[ResponseClassifier]]
   * to a [[com.twitter.finagle.Thrift.Server Thrift server]].
   */
  private[finagle] val ReqRepCtxOnly: ResponseClassifier =
    new ResponseClassifier {
      override def toString: String = "DefaultThriftResponseClassifier"

      // we want the side-effect of deserialization if it has not
      // yet been done
      private[this] def deserializeIfPossible(rep: Try[Any]): Unit = {
        rep match {
          case Return(bytes: Array[Byte]) =>
            val deserCtx = ServerToReqRep.get
            if (deserCtx ne ServerToReqRep.nullDeserializeCtx) {
              try {
                deserCtx(bytes).response
              } catch {
                case _: Throwable =>
              }
            }
          case _ =>
        }
      }

      def isDefinedAt(reqRep: ReqRep): Boolean = {
        deserializeIfPossible(reqRep.response)
        ResponseClassifier.Default.isDefinedAt(reqRep)
      }

      def apply(reqRep: ReqRep): ResponseClass = {
        deserializeIfPossible(reqRep.response)
        ResponseClassifier.Default(reqRep)
      }
    }
}
