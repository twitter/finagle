package com.twitter.finagle.postgresql

import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission

import com.twitter.io.StreamIO
import com.twitter.io.TempFile

import scala.jdk.CollectionConverters._

trait ResourceFileSpec { self: PgSqlSpec =>

  /**
   * Converts a resource file to a local temp file that an embedded pgsql instance can read.
   *
   * Permissions are set such that only the current user can read/write the file, this is necessary
   * for server certificates for example.
   */
  def toTmpFile(name: String): java.io.File =
    using(getClass.getResourceAsStream(name)) { is =>
      val file = TempFile.fromResourcePath(name)
      Files.setPosixFilePermissions(
        file.toPath,
        Set(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE).asJava
      )
      using(new java.io.FileOutputStream(file)) { os =>
        StreamIO.copy(is, os)
        file
      }
    }

}
