package com.twitter.finagle.exp.fiber_scheduler.util

import java.io.FileInputStream
import java.nio.charset.StandardCharsets
import java.util.Arrays
import java.util.regex.Pattern

import scala.util.control.NonFatal

import com.twitter.jvm.Jvm
import com.twitter.util.Duration

/**
 * Utility class to read information from the cgroup files.
 */
private[fiber_scheduler] final class Cgroup(rootPath: String, pid: String) {
  import Cgroup._
  def this() = this("", Cgroup.pid)

  private[this] var buffer = new Array[Byte](10240)

  private[this] def readLines[T](path: String): Array[String] = {
    var fis: FileInputStream = null
    try {
      fis = new FileInputStream(path)
      var read = 0
      var size = 0
      do {
        size += read
        var remaining = buffer.length - size
        if (remaining <= 0) {
          buffer = Arrays.copyOf(buffer, buffer.length << 1)
          remaining = buffer.length - size
        }
        read = fis.read(buffer, size, remaining)
      } while (read != -1)
      val string = new String(buffer, 0, size, StandardCharsets.UTF_8)
      val lines = lineBreakRegex.split(string)
      lines
    } catch {
      case NonFatal(_) =>
        new Array[String](0)
    } finally {
      if (fis != null) {
        fis.close()
      }
    }
  }

  private[this] val mountinfoLines = readLines(s"$rootPath/proc/$pid/mountinfo")
  private[this] val cgroupLines = readLines(s"$rootPath/proc/$pid/cgroup")

  private[this] val memoryFile: String => Option[String] = {
    val mountPath: Option[String] =
      mountinfoLines.collectFirst {
        case MountInfoMemory(p) => p
      }

    val cgroup: Option[String] =
      cgroupLines.collectFirst {
        case CgroupMemory(p) => p
      }

    (name: String) =>
      for {
        mount <- mountPath
        grp <- cgroup
      } yield s"$rootPath/$mount/$grp/$name"
  }

  private[this] val cpuFile: String => Option[String] = {
    val mountPath: Option[String] = {
      mountinfoLines.collectFirst {
        case MountInfoCpuCpuAcct(p) => p
        case MountInfoCpu(p) => p
      }
    }

    val cgroup: Option[String] =
      cgroupLines.collectFirst {
        case CgroupCpuCpuAcct(p) => p
        case CgroupCpu(p) => p
      }
    (name: String) =>
      for {
        mount <- mountPath
        grp <- cgroup
      } yield s"$rootPath/$mount/$grp/$name"
  }

  private[this] def readLong(path: String) =
    try {
      val lines = readLines(path)
      if (lines != null && lines.length == 1) {
        java.lang.Long.parseLong(lines(0).trim())
      } else {
        0L
      }
    } catch {
      case NonFatal(_) =>
        0L
    }

  val cpuPeriod: Duration =
    Duration.fromMicroseconds {
      cpuFile("cpu.cfs_period_us")
        .map(readLong)
        .getOrElse(0L)
    }

  val memoryUsagePercent: () => Double =
    memoryFile("memory.limit_in_bytes")
      .map(readLong) match {
      case Some(max) =>
        val usagePath = memoryFile("memory.usage_in_bytes").get
        () => readLong(usagePath).toDouble * 100 / max
      case None =>
        () => 0
    }

  val cpuNrThrottled: () => Long = {
    cpuFile("cpu.stat") match {
      case Some(path) =>
        val label = "nr_throttled "
        val labelLength = label.length
        () => {
          var i = 0
          var res = -1L
          val lines = readLines(path)
          while (lines != null && i < lines.length && res == -1) {
            val line = lines(i)
            if (line.startsWith(label)) {
              try {
                res = java.lang.Long.parseLong(line.substring(labelLength).trim)
              } catch {
                case NonFatal(_) =>
                  res = 0
              }
            }
            i += 1
          }
          if (res == -1) {
            res = 0
          }
          res
        }
      case None =>
        () => 0L
    }

  }
}

private final object Cgroup {
  private val pid: String =
    Jvm.ProcessId.map(_.toString).getOrElse("")

  // This is the CentOS7 format
  private val CgroupCpuCpuAcct = "^.*cpu,cpuacct:(.*)$".r
  // This is the CentOS5 format
  private val CgroupCpu = "^.*cpu:(.*)$".r

  // This is the CentOS5 format
  private val MountInfoCpu = "^.*\\s(/[^\\s]*cgroup/cpu)\\s.*$".r
  // This is the CentOS7 format
  private val MountInfoCpuCpuAcct = "^.*\\s(/[^\\s]*cgroup/cpu,cpuacct)\\s.*$".r

  private val MountInfoMemory = "^.*\\s(/[^\\s]*memory)\\s.*$".r
  private val CgroupMemory = "^.*memory:(.*)$".r

  private val lineBreakRegex = Pattern.compile("\\r?\\n")
}
