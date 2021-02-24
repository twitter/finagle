package com.twitter.finagle.mysql.unit.harness

import com.twitter.finagle.mysql.harness.MySqlExecutables
import java.io.File
import java.nio.file.{Path, Paths}
import org.mockito.Mockito.when
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

class MySqlExecutablesTest extends AnyFunSuite {
  test("retrieve mysqld and mysqlAdmin from a sequence of paths") {
    val executables = MySqlExecutables.fromExtractedFiles(
      Seq(
        Paths.get("/a/b/c"),
        Paths.get("/a/b/c/mysqld"),
        Paths.get("/a/b/c/mysqladmin"),
        Paths.get("/a/b/c/bin/mysqld"),
        Paths.get("/a/b/c/bin/mysqladmin")
      )
    )

    assert(executables.isDefined)
    assert(executables.get.toString.contains("/a/b/c/bin/mysqladmin"))
    assert(executables.get.toString.contains("/a/b/c/bin/mysqld"))
  }

  test("retrieve mysqld and mysqlAdmin from a path") {
    val mysqld: File = createMockFilePath("/a/b/c/bin/mysqld")
    val mysqlAdmin: File = createMockFilePath("/a/b/c/bin/mysqladmin")
    val dummyMysqld: File = createMockFilePath("notbin/mysqld")
    val dummyMysqlAdmin: File = createMockFilePath("notbin/mysqladmin")

    val mockDirectory: Path = createMockDirectoryPath(Array(dummyMysqld, dummyMysqlAdmin))
    val mockBinDirectory: Path = createMockDirectoryPath(Array(mysqlAdmin, mysqld))
    val mockRootDirectory: Path =
      createMockDirectoryPath(Array(mockDirectory.toFile, mockBinDirectory.toFile))

    val executables = MySqlExecutables.fromPath(mockRootDirectory)

    assert(executables.isDefined)
    assert(executables.get.toString.contains("/a/b/c/bin/mysqladmin"))
    assert(executables.get.toString.contains("/a/b/c/bin/mysqld"))
  }

  test("return None mysqld and mysqlAdmin are not found in a path") {
    val dummyMysqld: File = createMockFilePath("notbin/mysqld")
    val dummyMysqlAdmin: File = createMockFilePath("notbin/mysqladmin")

    val mockDirectory: Path = createMockDirectoryPath(Array(dummyMysqld, dummyMysqlAdmin))
    val mockRootDirectory: Path =
      createMockDirectoryPath(Array(mockDirectory.toFile))

    assert(MySqlExecutables.fromPath(mockRootDirectory).isEmpty)
  }

  private def createMockDirectoryPath(childFiles: Array[File]): Path = {
    val mockPath = mock[Path]
    val mockFile = mock[File]
    when(mockPath.toFile).thenReturn(mockFile)
    when(mockFile.isDirectory).thenReturn(true)
    when(mockFile.isFile).thenReturn(false)
    when(mockFile.listFiles()).thenReturn(childFiles)
    mockPath
  }

  private def createMockFilePath(path: String): File = {
    val mockPath = mock[Path]
    val mockFile = mock[File]
    when(mockFile.isDirectory).thenReturn(false)
    when(mockFile.isFile).thenReturn(true)
    when(mockFile.toPath).thenReturn(mockPath)
    when(mockPath.toString).thenReturn(path)
    when(mockPath.toFile).thenReturn(mockFile)
    when(mockFile.setExecutable(true)).thenReturn(true)
    mockFile
  }
}
