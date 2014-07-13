package com.twitter.finagle.redis

import com.twitter.finagle.redis.protocol._
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffer

trait Scripts { self: BaseClient =>

  /**
   * Execute a Lua script server side
   * @param script
   * @param numkeys
   * @param keys
   * @param args
   * @return result of evaluating the script
   */
  def eval() {

  }

  /**
   * Execute a Lua script server side by its SHA1 digest
   * @param sha1
   * @param numkeys
   * @param keys
   * @param args
   * @return result of evaluating the script
   */
  def evalSHA() {

  }

  /**
   * Checks if scripts exists in the script cache
   * @param scripts
   * @return
   */
  def scriptExists() {

  }

  /**
   * Flush the script cache
   * @return
   */
  def scriptFlush() {

  }

  /**
   * Kill the currently executing script
   * @return
   */
  def scriptKill() {

  }

  /**
   * Load a script into the scripts cache
   * @param script
   * @return
   */
  def scriptLoad() {

  }
}
