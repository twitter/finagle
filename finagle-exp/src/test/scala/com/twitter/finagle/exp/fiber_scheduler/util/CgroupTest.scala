package com.twitter.finagle.exp.fiber_scheduler.util

import com.twitter.io.TempDirectory
import com.twitter.util.Duration
import com.twitter.finagle.exp.fiber_scheduler.FiberSchedulerSpec
import java.io.File
import java.net.URI
import java.util.jar.JarFile
import org.apache.commons.io.FileUtils

class CgroupTest extends FiberSchedulerSpec {
  val placeholder = "com/twitter/finagle/exp/fiber_scheduler/placeholder"
  val root = {
    val filename =
      getClass.getClassLoader.getResource(placeholder).getFile

    if (filename.contains('!')) {
      // Bazel export files from jar to temp directory
      val fn = filename.split('!').head
      val uri = new URI(fn)
      val jarFile = new JarFile(new File(uri.getPath))
      val entries = jarFile.entries()
      val tempDirectory = TempDirectory.create()
      val tempDirectoryPath = tempDirectory.getAbsolutePath

      while (entries.hasMoreElements) {
        val nextElement = entries.nextElement()

        if (!nextElement.isDirectory) {
          val file = new File(tempDirectoryPath, nextElement.getName)
          val inputStream = jarFile.getInputStream(nextElement)
          FileUtils.copyInputStreamToFile(inputStream, file)
          inputStream.close()
        }
      }

      val schedulerPath =
        new File(tempDirectoryPath, placeholder).getParent
      schedulerPath
    } else {
      // Get path from bazel based resources folder
      val schedulerPath =
        getClass.getClassLoader.getResource(placeholder).getPath
      new File(schedulerPath).getParent
    }
  }

  "CentOS5" - {
    val cgroup = new Cgroup(root, "1")
    "cpuPeriod" in {
      assert(cgroup.cpuPeriod == Duration.fromMilliseconds(100))
    }
    "memoryUsagePercent" in {
      assert((cgroup.memoryUsagePercent() * 100).toInt == 888)
    }
    "cpuNrThrottled" in {
      assert(cgroup.cpuNrThrottled() == 230)
    }
  }

  "CentOS7" - {
    val cgroup = new Cgroup(root, "2")
    "cpuPeriod" in {
      assert(cgroup.cpuPeriod == Duration.fromMilliseconds(100))
    }
    "memoryUsagePercent" in {
      assert((cgroup.memoryUsagePercent() * 100).toInt == 888)
    }
    "cpuNrThrottled" in {
      assert(cgroup.cpuNrThrottled() == 230)
    }
  }

  "invalid" - {
    val cgroup = new Cgroup(root, "3")
    "cpuPeriod" in {
      assert(cgroup.cpuPeriod == Duration.fromMilliseconds(0))
    }
    "memoryUsagePercent" in {
      assert(cgroup.memoryUsagePercent() == 0)
    }
    "cpuNrThrottled" in {
      assert(cgroup.cpuNrThrottled() == 0)
    }
  }
}
