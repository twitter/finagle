package com.twitter.finagle.util

// This is a purpose-built data structure to use with Drv construction.
// It's core optimization is that we know the small and large sets, at their
// peak size, have the same number of total elements as weights in the Drv
// so we can pre-allocate an array to back this. This avoids the boxing and
// noise that is necessary for general purpose data structures.
private final class DrvSmallLargeSets(N: Int) {
  private[this] val data = new Array[Int](N)

  // These are "read" indexes, meaning that the next read operation will come
  // from the index as it is now.
  private[this] var front = -1
  private[this] var back = N

  // If we want to add an element one of the two indices will increment
  // toward the other and then place the element there. If the indexes
  // are already next to each other we'll get a collision and data
  // corruption. This should never happen as at most we'll ever have N
  // elements in the stacks right after filling them up.
  private[this] def checkOverflow(): Unit = {
    if (back - front <= 1) {
      throw new IllegalStateException("overflow")
    }
  }

  def smallIsEmpty: Boolean = front == -1

  def largeIsEmpty: Boolean = back == N

  def smallPush(i: Int): Unit = {
    checkOverflow()
    front += 1
    data(front) = i
  }

  def largePush(i: Int): Unit = {
    checkOverflow()
    back -= 1
    data(back) = i
  }

  def smallPop(): Int = {
    if (smallIsEmpty) {
      throw new IllegalStateException("empty small queue")
    }

    val r = data(front)
    front -= 1
    r
  }

  def largePop(): Int = {
    if (largeIsEmpty) {
      throw new IllegalStateException("empty large qeueue")
    }
    val r = data(back)
    back += 1
    r
  }
}
