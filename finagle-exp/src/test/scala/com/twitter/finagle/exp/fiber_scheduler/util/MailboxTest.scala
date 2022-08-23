package com.twitter.finagle.exp.fiber_scheduler.util

import com.twitter.finagle.exp.fiber_scheduler.FiberSchedulerSpec

class MailboxTest extends FiberSchedulerSpec {

  "asOwner" - {
    "add" - {
      "owner only" - test { m =>
        m.asOwnerAdd(1)
        m.asOwnerAdd(2)
        if (m.fifo) {
          checkItems(m, List(1, 2))
        } else {
          checkItems(m, List(2, 1))
        }
      }
      "with other" - test { m =>
        m.asOtherAdd(1)
        m.asOwnerAdd(2)
        if (m.fifo) {
          checkItems(m, List(1, 2))
        } else {
          checkItems(m, List(2, 1))
        }
      }
    }
    "addLast" - {
      "owner only" - test { m =>
        m.asOwnerAddLast(1)
        m.asOwnerAddLast(2)
        checkItems(m, List(1, 2))
      }
      "with other" - test { m =>
        m.asOtherAdd(1)
        m.asOwnerAddLast(2)
        checkItems(m, List(1, 2))
      }
    }
    "addFirst" - {
      "owner only" - test { m =>
        m.asOwnerAddFirst(1)
        m.asOwnerAddFirst(2)
        checkItems(m, List(2, 1))
      }
      "with other" - test { m =>
        m.asOtherAdd(1)
        m.asOwnerAddFirst(2)
        checkItems(m, List(2, 1))
      }
    }
    "poll" - test { m =>
      m.asOtherAdd(1)
      m.asOwnerAddFirst(2)
      checkItems(m, List(2, 1))
      m.asOtherAdd(3)
      checkItems(m, List(3))
    }
    "close" - test { m =>
      m.asOwnerAdd(1)
      m.asOwnerAdd(2)
      val l = m.asOwnerClose()
      assert(m.isEmpty())
      assert(m.isClosed())
      if (m.fifo) {
        assert(l == List(1, 2))
      } else {
        assert(l == List(2, 1))
      }
    }
  }

  "asOther" - {
    "add" - {
      "other only" - test { m =>
        m.asOtherAdd(1)
        m.asOtherAdd(2)
        if (m.fifo) {
          checkItems(m, List(1, 2))
        } else {
          checkItems(m, List(2, 1))
        }
      }
      "with owner" - test { m =>
        m.asOwnerAdd(1)
        m.asOtherAdd(2)
        m.asOwnerAdd(3)
        if (m.fifo) {
          checkItems(m, List(1, 2, 3))
        } else {
          checkItems(m, List(3, 2, 1))
        }
      }
    }
    "steal" - {
      "from inbox" - test { m =>
        m.asOtherAdd(1)
        assert(m.asOtherSteal() == 1)
      }
      "from items if unrestricted" - test { m =>
        if (!m.isRestricted()) {
          m.asOwnerAdd(1)
          assert(m.asOtherSteal() == 1)
        }
      }
    }
  }

  "reads large inbox without a stack overflow" - test { m =>
    val l = (1 until 1000).toList
    l.foreach(m.asOtherAdd(_))
    if (m.fifo) {
      checkItems(m, l)
    } else {
      checkItems(m, l.reverse)
    }
  }

  private def checkItems(m: Mailbox[Integer], l: List[Int]) = {
    var r = List.empty[Int]
    var v = m.asOwnerPoll()
    while (v != null) {
      r ::= v
      v = m.asOwnerPoll()
    }
    assert(r.reverse == l)
  }

  private def test(f: Mailbox[Integer] => Unit) =
    for {
      fifo <- List(true, false)
      restricted <- List(true, false)
    } {
      s"fifo=$fifo restricted=$restricted" in {
        f(Mailbox(fifo, restricted))
      }
    }
}
