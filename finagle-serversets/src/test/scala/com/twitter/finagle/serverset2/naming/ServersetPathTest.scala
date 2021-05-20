package com.twitter.finagle.serverset2.naming

import com.twitter.finagle.Path
import org.scalatest.funsuite.AnyFunSuite

class ServersetPathTest extends AnyFunSuite {
  test("of") {
    assert(
      ServersetPath.of(Path.read("/hosts")) == Some(ServersetPath("hosts", Path.empty, None, None))
    )

    assert(
      ServersetPath.of(Path.read("/hosts/foo/bar")) == Some(
        ServersetPath("hosts", Path.Utf8("foo", "bar"), None, None)
      )
    )

    assert(
      ServersetPath.of(Path.read("/hosts/foo/bar:endpoint")) == Some(
        ServersetPath("hosts", Path.Utf8("foo", "bar"), Some("endpoint"), None)
      )
    )

    assert(
      ServersetPath.of(Path.read("/hosts/foo/bar#123")) == Some(
        ServersetPath("hosts", Path.Utf8("foo", "bar"), None, Some(123))
      )
    )

    assert(
      ServersetPath.of(Path.read("/hosts/foo/bar:endpoint#123")) == Some(
        ServersetPath("hosts", Path.Utf8("foo", "bar"), Some("endpoint"), Some(123))
      )
    )

    assert(ServersetPath.of(Path.empty).isEmpty)

    intercept[IllegalArgumentException] {
      ServersetPath.of(Path.read("/hosts/bar#123:endpoint"))
    }

    intercept[IllegalArgumentException] {
      ServersetPath.of(Path.read("/hosts/bar#abc"))
    }
  }
}
