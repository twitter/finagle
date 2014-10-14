namespace java com.twitter.finagle.kestrel.net.lag.kestrel.thrift
#@namespace scala com.twitter.finagle.kestrel.net.lag.kestrel.thriftscala

struct Item {
  /* the actual data */
  1: binary data

  /* transaction ID, to be used in the `confirm` call */
  2: i64 id
}

struct QueueInfo {
  /* the head item on the queue, if there is one */
  1: optional binary head_item

  /* # of items currently in the queue */
  2: i64 items

  /* total bytes of data currently in the queue */
  3: i64 bytes

  /* total bytes of journal currently on-disk */
  4: i64 journal_bytes

  /* age (in milliseconds) of the head item on the queue, if present */
  5: i64 age

  /* # of clients currently waiting to fetch an item */
  6: i32 waiters

  /* # of items that have been fetched but not confirmed */
  7: i32 open_transactions
}

enum Status {
  /* Server status not configured, status levels not in use. */
  NOT_CONFIGURED = 0,

  /* Server marked quiescent -- clients should stop reading and writing. */
  QUIESCENT = 1,

  /* Server marked read only -- clients should stop writing. */
  READ_ONLY = 2,

  /* Server marked up -- clients may read or write. */
  UP = 3,
}

service Kestrel {
  /*
   * Put one or more items into a queue.
   *
   * If the named queue doesn't exist, it will be created.
   *
   * Optionally, an expiration time can be set on the items. If they sit in
   * the queue without being fetched for longer than the expiration, then
   * they will vanish.
   *
   * Returns the number of items actually put. This may be fewer than were
   * requested if the queue has a size/length limit and its policy when full
   * is to refuse new items.
   */
  i32 put(1: string queue_name, 2: list<binary> items, 3: i32 expiration_msec = 0)

  /*
   * Get one or more items from a queue.
   *
   * If the timeout is set, then this call will block until at least
   * `max_items` have been fetched, or the timeout occurs. If the timeout
   * occurs, this call may return from zero to `max_items` items.
   *
   * With no timeout, the call will return only with items that are
   * immediately available.
   *
   * If `auto_abort_msec` is 0 (the default), the fetched items will behave
   * as if a `confirm` call has been made for them already: they will be
   * permanently removed from the queue. The `id` field in each `Item` will
   * be zero. Otherwise, the client must call `confirm` with the same `id`
   * before `auto_abort_msec` milliseconds have elapsed or the item will be
   * re-enqueued (as if `abort` had been called).
   *
   * If you exceed the maxmimum open reads for a connection, `get` will stop
   * returning items until 1 or more is confirmed or aborted.
   */
  list<Item> get(1: string queue_name, 2: i32 max_items, 3: i32 timeout_msec = 0, 4: i32 auto_abort_msec = 0)

  /*
   * Confirm a set of items previously fetched with `get`.
   * Returns the count of confirmed items.
   */
  i32 confirm(1: string queue_name, 2: set<i64> ids)

  /*
   * Abort a set of items previously fetched with `get`.
   * Returns the count of aborted items.
   */
  i32 abort(1: string queue_name, 2: set<i64> ids)

  /*
   * Return some basic info about a queue, and the head item if there is
   * at least one item in the queue. The item is not dequeued, and there is
   * no guarantee that the item still exists by the time this method
   * returns.
   */
  QueueInfo peek(1: string queue_name)

  /*
   * Flush (clear out) a queue. All unfetched items are lost.
   */
  void flush_queue(1: string queue_name)

  /*
   * Flush (clear out) ALL QUEUES. All unfetched items from all queues are
   * lost.
   */
  void flush_all_queues()

  /*
   * Delete a queue, removing any journal. All unfetched items are lost.
   * ("delete" is a reserved word in some thrift variants.)
   */
  void delete_queue(1: string queue_name)

  /*
   * Retrieve server's current advertised status.
   */
  Status current_status()

  /*
   * Set the server's current status. Throws an exception if server status
   * is not configured.
   */
  void set_status(1: Status status)

  /*
   * Return a string form of the version of this kestrel server.
   */
  string get_version()
}
