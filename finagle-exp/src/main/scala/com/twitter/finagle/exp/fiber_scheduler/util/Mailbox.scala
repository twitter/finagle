package com.twitter.finagle.exp.fiber_scheduler.util

import java.util.Arrays
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicReferenceArray

import com.twitter.finagle.exp.fiber_scheduler.Config

import sun.misc.Unsafe

/**
 * A queue implementation optimized for the case where an owner
 * thread performs most of the operations. The mailbox maintains an internal
 * data structure that is used only by the owner. Other threads use a concurrent
 * inbox that is eventually read by the owner.
 *
 * @param fifo uses fifo order if true, lifo if false
 * @tparam T the type of the value
 */
private[fiber_scheduler] abstract class Mailbox[T] private (protected[util] val fifo: Boolean) {

  // inbox used to stash items added by other threads
  protected[this] final val inbox = new AtomicReference(List.empty[T])

  // Methods to be used only by the owner. The mailbox implementation doesn't
  // enforce ownership and the caller code must handle proper access

  def asOwnerAdd(v: T): Unit =
    if (fifo) {
      asOwnerAddLast(v)
    } else {
      asOwnerAddFirst(v)
    }

  def asOwnerAddLast(v: T): Unit = {
    if (fifo) transferFromInbox()
    addLast(v)
  }

  private def addLast(v: T): Unit = {
    val t = tail
    setItem(t, v)
    val nt = mask(t + 1)
    if (nt != head) {
      setTail(nt)
    } else {
      grow(head)
    }
  }

  def asOwnerAddFirst(v: T): Unit = {
    if (!fifo) transferFromInbox()
    addFirst(v)
  }

  private def addFirst(v: T): Unit = {
    val nh = mask(head - 1)
    setItem(nh, v)
    if (nh != tail) {
      setHead(nh)
    } else {
      grow(nh)
    }
  }

  def asOwnerPoll(): T = {
    if (!fifo) transferFromInbox()
    var v = pollFirst()
    if (v == null && fifo) {
      transferFromInbox()
      v = pollFirst()
    }
    v
  }

  def asOwnerClose(): List[T] = {
    var r = inbox.getAndSet(null)
    var t = pollFirst()
    while (t != null) {
      r ::= t
      t = pollFirst()
    }
    r.reverse
  }

  private[this] def pollFirst(): T = {
    var nh = head
    var v = null.asInstanceOf[T]
    while (v == null && nh != tail) {
      v = removeItem(nh)
      nh = mask(nh + 1)
    }
    if (v != null) {
      setHead(nh)
    }
    v
  }

  private[this] def transferFromInbox() = {
    var i = inbox.get
    while (i != null && (i ne Nil) && !inbox.compareAndSet(i, Nil)) {
      i = inbox.get
    }
    def add(v: T) =
      if (fifo) {
        addLast(v)
      } else {
        addFirst(v)
      }
    if (i != null) {
      // use the stack to read the inbox list in reverse
      // without having to call list.reverse, which has a higher
      // cost due to allocations
      val maxDepth = Config.Mailbox.readMaxStackDepth
      def loop(i: List[T], depth: Int): Unit = {
        if (i ne Nil) {
          if (depth == maxDepth) {
            var l = i.reverse
            while (l ne Nil) {
              add(l.head)
              l = l.tail
            }
          } else {
            loop(i.tail, depth + 1)
            add(i.head)
          }
        }
      }
      loop(i, 0)
    }
  }

  // Methods that can be used by other threads

  def asOtherAdd(v: T): Boolean = {
    var i = inbox.get
    while (i != null && !inbox.compareAndSet(i, v :: i)) {
      i = inbox.get
    }
    i != null
  }

  def asOtherSteal(): T = {
    var i = inbox.get
    while (i != null && (i ne Nil) && !inbox.compareAndSet(i, i.tail)) {
      i = inbox.get
    }
    if (i != null && (i ne Nil)) {
      i.head
    } else {
      null.asInstanceOf[T]
    }
  }

  // Methods to handle the internal data structure

  protected[this] def head: Int
  protected[this] def tail: Int
  protected[this] def setHead(i: Int): Unit
  protected[this] def setTail(i: Int): Unit
  protected[this] def setItem(i: Int, v: T): Unit
  protected[this] def getItem(i: Int): T
  protected[this] def removeItem(i: Int): T
  protected[this] def itemsLength: Int
  protected[this] def grow(head: Int): Unit

  def isEmpty(): Boolean
  protected[util] def isRestricted(): Boolean = this.isInstanceOf[Mailbox.Restricted[_]]

  final def isClosed(): Boolean = {
    inbox.get == null
  }

  final def size(): Int = {
    itemsSize() + inboxSize()
  }

  final def itemsSize(): Int = {
    mask(tail - head)
  }

  final def inboxSize(): Int = {
    var r = 0
    var i = inbox.get
    if (i != null && (i ne Nil)) {
      r += 1
      i = i.tail
    }
    r
  }

  protected[this] final def mask(i: Int) = i & (itemsLength - 1)

  override final def toString = {
    val s = itemsSize()
    val a = new Array[Any](s)
    var i = 0
    while (i < s) {
      a(i) = getItem(mask(head + i))
      i += 1
    }
    val items = Arrays.toString(a.asInstanceOf[Array[Object]])
    s"Mailbox(items=$items, inbox = $inbox)"
  }
}

private[fiber_scheduler] final object Mailbox {

  def apply[T](fifo: Boolean, restricted: Boolean): Mailbox[T] =
    if (restricted) new Restricted(fifo)
    else new Unrestricted(fifo)

  /**
   * A restricted mailbox uses a regular array and doesn't allow other
   * threads to steal from its internal data structure
   */
  private final class Restricted[T](fifo: Boolean) extends Mailbox[T](fifo) {
    private[this] var items = (new Array[Any](8)).asInstanceOf[Array[T]]
    protected[this] var head = 0
    protected[this] var tail = 0

    override protected[this] def setHead(i: Int) = head = i
    override protected[this] def setTail(i: Int) = tail = i
    override protected[this] def setItem(i: Int, v: T) = {
      assert(items(i) == null)
      items(i) = v
    }
    override protected[this] def getItem(i: Int) = items(i)
    override protected[this] def itemsLength = items.length

    override protected[this] def removeItem(i: Int): T = {
      val v = items(i)
      items(i) = null.asInstanceOf[T]
      v
    }

    override def isEmpty(): Boolean = {
      val i = inbox.get()
      head == tail && ((i eq Nil) || i == null)
    }

    override protected[this] def grow(head: Int): Unit = {
      val size = items.length
      val a = (new Array[Any](size << 1)).asInstanceOf[Array[T]]
      val r = size - head
      System.arraycopy(items, head, a, 0, r)
      System.arraycopy(items, 0, a, r, head)
      items = a
      setHead(0)
      setTail(size)
    }

  }

  /**
   * An unrestricted mailbox uses an atomic array and allows other
   * threads to steal from its internal data structure
   */
  private final class Unrestricted[T](fifo: Boolean) extends Mailbox[T](fifo) {
    import Unrestricted._

    @volatile protected[this] var items = new AtomicReferenceArray[T](8 << arrayPadding)
    @volatile protected[this] var head = 0
    @volatile protected[this] var tail = 0

    override protected[this] def setHead(i: Int) = head = i
    override protected[this] def setTail(i: Int) = tail = i
    override protected[this] def setItem(i: Int, v: T) = {
      assert(items.get(i) == null)
      items.set(pad(i), v)
    }
    override protected[this] def getItem(i: Int): T = items.get(pad(i))
    override protected[this] def itemsLength = items.length >> arrayPadding

    override protected[this] def removeItem(i: Int): T = {
      var v = getItem(i)
      while (v != null && !casItem(i, v, null.asInstanceOf[T])) {
        v = getItem(i)
      }
      v
    }

    override def isEmpty(): Boolean = {
      val i = inbox.get()
      if ((i ne Nil) && i != null) {
        false
      } else {
        var j = 0
        val it = items
        while (j < it.length()) {
          if (it.get(j) != null) {
            return false
          }
          j += 1
        }
        true
      }
    }

    override def asOtherSteal() = {
      var v = super.asOtherSteal()
      if (v == null) {
        var i = mask(tail - 1)
        do {
          v = getItem(i)
          if (v != null && !casItem(i, v, null.asInstanceOf[T])) {
            v = null.asInstanceOf[T]
          }
          i = mask(i - 1)
        } while (v == null && i != head)
      }
      v
    }

    override protected[this] def grow(head: Int): Unit = {
      val a = new AtomicReferenceArray[T](items.length << 1)
      var i = 0
      val l = itemsLength
      while (i < l) {
        val j = (head + i) & (l - 1)
        a.set(pad(i), removeItem(j))
        i += 1
      }
      setHead(0)
      setTail(l)
      items = a
    }

    private[this] def pad(i: Int) =
      i << arrayPadding

    private[this] def casItem(i: Int, expected: T, value: T): Boolean =
      items.compareAndSet(pad(i), expected, value)

  }

  private[this] final object Unrestricted {
    // Pad the atomic array to avoid false sharing.
    // Enabled by configuration.
    val arrayPadding =
      if (Config.Mailbox.padUnrestrictedMailboxes) {
        val unsafe = {
          val f = classOf[Unsafe].getDeclaredField("theUnsafe")
          f.setAccessible(true)
          f.get(null).asInstanceOf[Unsafe]
        }
        val arrayScale = unsafe.arrayIndexScale(classOf[Array[Object]])

        31 - Integer.numberOfLeadingZeros(64 / arrayScale)
      } else {
        0
      }
  }
}
