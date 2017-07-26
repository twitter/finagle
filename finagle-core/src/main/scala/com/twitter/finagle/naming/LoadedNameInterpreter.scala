package com.twitter.finagle.naming

import com.twitter.finagle.{Dtab, Path, Name, NameTree}
import com.twitter.finagle.util.LoadService
import com.twitter.util.Activity

class MultipleNameInterpretersException(val interpreters: Seq[NameInterpreter])
    extends IllegalStateException(
      s"Multiple `NameInterpreter`s found: ${interpreters.mkString(", ")}"
    )

// Note: exposed for testing.
private[naming] class LoadedNameInterpreter(load: () => Seq[NameInterpreter])
    extends NameInterpreter {

  private[this] val self: NameInterpreter = {
    load().toList match {
      case Nil => DefaultInterpreter
      case interpreter :: Nil => interpreter
      case interpreters =>
        throw new MultipleNameInterpretersException(interpreters)
    }
  }

  final def bind(dtab: Dtab, path: Path): Activity[NameTree[Name.Bound]] =
    self.bind(dtab, path)
}

/**
 * A [[NameInterpreter]] that delegates to a service-loaded
 * [[NameInterpreter]].
 *
 * The existence of multiple service-loaded [[NameInterpreters]] is
 * an illegal state. If no service-loaded [[NameInterpreter]]s are
 * found, use the [[DefaultInterpreter]].
 */
object LoadedNameInterpreter extends LoadedNameInterpreter(() => LoadService[NameInterpreter]())
