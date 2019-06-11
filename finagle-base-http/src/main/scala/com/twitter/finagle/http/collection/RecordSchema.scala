package com.twitter.finagle.http.collection

import java.util.IdentityHashMap

/**
 * RecordSchema represents the declaration of a heterogeneous
 * [[com.twitter.finagle.http.collection.RecordSchema.Record Record]] type, with
 * [[com.twitter.finagle.http.collection.RecordSchema.Field Fields]] that are determined at runtime. A Field
 * declares the static type of its associated values, so although the record itself is dynamic,
 * field access is type-safe.
 *
 * Given a RecordSchema declaration `schema`, any number of
 * [[com.twitter.finagle.http.collection.RecordSchema.Record Records]] of that schema can be obtained with
 * `schema.newRecord`. The type that Scala assigns to this value is what Scala calls a
 * "[[https://lampwww.epfl.ch/~amin/dot/fpdt.pdf path-dependent type]]," meaning that
 * `schema1.Record` and `schema2.Record` name distinct types. The same is true of fields:
 * `schema1.Field[A]` and `schema2.Field[A]` are distinct, and can only be used with the
 * corresponding Record.
 */
final class RecordSchema {

  private[RecordSchema] final class Entry(@volatile var value: Any) {
    @volatile var locked: Boolean = false
  }

  /**
   * Record is an instance of a [[com.twitter.finagle.http.collection.RecordSchema RecordSchema]] declaration.
   * Records are mutable; the `update` method assigns or reassigns a value to a given field. If
   * the user requires that a field's assigned value is never reassigned later, the user can `lock`
   * that field.
   */
  final class Record private[RecordSchema] (
    // note: modifications to `_fields` must be synchronized as `IdentityHashMap` itself
    // is not thread-safe.
    private[this] var _fields: IdentityHashMap[Field[_], Entry] = null) {

    private[this] def fields: IdentityHashMap[Field[_], Entry] = {
      if (_fields == null) {
        // start w/small expected size (5 mappings), allowing it to grow as needed.
        _fields = new IdentityHashMap[Field[_], Entry](5)
      }
      _fields
    }

    private[this] def getOrInitializeEntry(field: Field[_]): Entry = {
      synchronized {
        var entry = fields.get(field)
        if (entry eq null) {
          entry = new Entry(field.default())
          fields.put(field, entry)
        }
        entry
      }
    }

    /**
     * Returns the current value assigned to `field` in this record. If there is no value currently
     * assigned (either explicitly by a previous `update`, or by the field's declared default), this
     * will throw an `IllegalStateException`, indicating the field was never initialized.
     *
     * Note that Scala provides two syntactic equivalents for invoking this method:
     *
     * {{{
     * record.apply(field)
     * record(field)
     * }}}
     *
     * @param field the field to access in this record
     * @return the value associated with `field`.
     * @throws IllegalStateException
     */
    @throws(classOf[IllegalStateException])
    def apply[A](field: Field[A]): A =
      getOrInitializeEntry(field).value.asInstanceOf[A]

    /**
     * Locks the current value for a given `field` in this record, preventing further `update`s. If
     * there is no value currently assigned (either explicitly by a previous `update`, or by the
     * field's declared default), this will throw an `IllegalStateException`, indicating the field
     * was never initialized.
     *
     * @param field the field to lock in this record
     * @return this record
     * @throws IllegalStateException
     */
    @throws(classOf[IllegalStateException])
    def lock(field: Field[_]): Record = {
      getOrInitializeEntry(field).locked = true
      this
    }

    /**
     * Assigns (or reassigns) a `value` to a `field` in this record. If this field was previously
     * `lock`ed, this will throw an `IllegalStateException` to indicate failure.
     *
     * Note that Scala provides two syntactic equivalents for invoking this method:
     *
     * {{{
     * record.update(field, value)
     * record(field) = value
     * }}}
     *
     * @param field the field to assign in this record
     * @param value the value to assign to `field` in this record
     * @return this record
     * @throws IllegalStateException
     */
    @throws(classOf[IllegalStateException])
    def update[A](field: Field[A], value: A): Record = {
      synchronized {
        val entry = fields.get(field)
        if (entry eq null) {
          fields.put(field, new Entry(value))
        } else if (entry.locked) {
          throw new IllegalStateException(
            s"attempt to assign $value to a locked field (with current value ${entry.value})"
          )
        } else {
          entry.value = value
        }
      }
      this
    }

    /**
     * Assigns (or reassigns) a `value` to a `field` in this record, and locks it to prevent further
     * `update`s. This method is provided for convenience only; the following are equivalent:
     *
     * {{{
     * record.updateAndLock(field, value)
     * record.update(field, value).lock(field)
     * }}}
     *
     * @param field the field to assign and lock in this record
     * @param value the value to assign to `field` in this record
     * @return this record
     * @throws IllegalStateException
     */
    @throws(classOf[IllegalStateException])
    def updateAndLock[A](field: Field[A], value: A): Record =
      update(field, value).lock(field)

    private[this] def copyFields(): IdentityHashMap[Field[_], Entry] = {
      synchronized {
        val newFields = new IdentityHashMap[Field[_], Entry](fields.size)
        val iter = fields.entrySet().iterator()
        while (iter.hasNext) {
          val kv = iter.next()
          val entry = kv.getValue()
          val newEntry = new Entry(entry.value)
          newEntry.locked = entry.locked
          newFields.put(kv.getKey(), newEntry)
        }
        newFields
      }
    }

    /**
     * Create a copy of this record.  Fields are locked in the copy if and only if they
     * were locked in the original record.
     *
     * @return a copy of this record
     */
    def copy(): Record = {
      new Record(copyFields())
    }

    /**
     * Create a copy of this record with `value` assigned to `field`.  `field` will be locked in the
     * copy if and only if it was present and locked in the original record.  If `field` was not
     * present in the original then the following are equivalent:
     *
     * {{{
     * record.copy(field, value)
     * record.copy().update(field, value)
     * }}}
     *
     * @param field the field to assign in the copy
     * @param value the value to assign to `field` in the copy
     * @return a copy of this record
     */
    def copy[A](field: Field[A], value: A): Record = {
      val newFields = copyFields()
      val entry = newFields.get(field)
      if (entry eq null) {
        newFields.put(field, new Entry(value))
      } else {
        entry.value = value
      }
      new Record(newFields)
    }
  }

  /**
   * Creates a new [[com.twitter.finagle.http.collection.RecordSchema.Record Record]] from this Schema.
   *
   * @return a new [[com.twitter.finagle.http.collection.RecordSchema.Record Record]]
   */
  def newRecord(): Record = new Record

  /**
   * Field is a handle used to access some corresponding value in a
   * [[com.twitter.finagle.http.collection.RecordSchema.Record Record]]. A field may also declare a default,
   * which is computed for each record it is associated with, at most once per record instance.
   */
  sealed trait Field[A] {
    def default(): A
  }

  /**
   * Creates a new [[com.twitter.finagle.http.collection.RecordSchema.Field Field]] with no default value, to be
   * used only with [[com.twitter.finagle.http.collection.RecordSchema.Record Records]] from this schema.
   *
   * @return a [[com.twitter.finagle.http.collection.RecordSchema.Field Field]] with no default value
   */
  def newField[A](): Field[A] = new Field[A] {
    override def default(): A =
      throw new IllegalStateException("attempt to access uninitialized field")
  }

  /**
   * Creates a new [[com.twitter.finagle.http.collection.RecordSchema.Field Field]] with the given
   * `defaultSupplier`, to be used only with [[com.twitter.finagle.http.collection.RecordSchema.Record Records]]
   * from this schema.
   *
   * @param defaultSupplier a computation producing the default value to use, when there is no value
   *        previously assigned to this field in a given record.
   * @return a [[com.twitter.finagle.http.collection.RecordSchema.Field Field]] with the given `defaultSupplier`
   */
  def newField[A](defaultSupplier: => A): Field[A] = new Field[A] {
    override def default(): A = defaultSupplier
  }
}
