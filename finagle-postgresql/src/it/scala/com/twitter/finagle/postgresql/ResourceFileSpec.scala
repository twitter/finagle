package com.twitter.finagle.postgresql

import com.twitter.io.StreamIO

trait ResourceFileSpec { self: PgSqlSpec =>

  /**
   * Converts a resource file to a local temp file that an embedded pgsql instance can read.
   *
   * Permissions are set such that only the current user can read/write the file, this is necessary
   * for server certificates for example.
   */
  def toTmpFile(name: String): java.io.File =
    using(getClass.getResourceAsStream(name)) { is =>
      // NOTE: we have to hardcode `/tmp` because Docker cannot read files from the OSX tmp sandbox.
      // This makes the test suite more platform-specific,
      //   but it should work on most system without any modifications to the docker daemon
      val file = java.io.File.createTempFile(name, null, new java.io.File("/tmp"))
      using(new java.io.FileOutputStream(file)) { os =>
        StreamIO.copy(is, os)
        file
      }
    }

}
