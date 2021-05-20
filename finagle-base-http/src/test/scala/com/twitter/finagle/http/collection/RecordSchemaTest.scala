package com.twitter.finagle.http.collection

import org.scalatest.funsuite.AnyFunSuite

class RecordSchemaTest extends AnyFunSuite {

  private val schema = new RecordSchema
  private val field = schema.newField[Object]()
  private val fieldWithDefault = schema.newField[Object](new Object)
  private val fields = Seq(field, fieldWithDefault)

  test("apply should throw IllegalStateException when field is uninitialized") {
    val record = schema.newRecord()
    intercept[IllegalStateException] {
      record(field)
    }
  }

  test("apply should compute, store and return default when field is initialized with default") {
    val record = schema.newRecord()
    assert(record(fieldWithDefault) eq record(fieldWithDefault))
  }

  test("apply should return field value when field is explicitly initialized") {
    val record = schema.newRecord()
    val value = new Object

    for (f <- fields) {
      record(f) = value
      assert(record(f) eq value)
    }
  }

  test("lock should throw IllegalStateException when field is uninitialized") {
    val record = schema.newRecord()
    intercept[IllegalStateException] {
      record.lock(field)
    }
  }

  test("lock should compute and store default when field is initialized with default") {
    val record = schema.newRecord()
    record.lock(fieldWithDefault)
    assert(record(fieldWithDefault) ne null)
  }

  test("update should reassign when field is not locked") {
    val record = schema.newRecord()
    val value = new Object

    for (f <- fields) {
      record(f) = new Object
      record(f) = value
      assert(record(f) eq value)
    }
  }

  test("update should throw IllegalStateException when field is locked") {
    val record = schema.newRecord()
    val value = new Object

    for (f <- fields) {
      record(f) = value
      record.lock(f)
      intercept[IllegalStateException] {
        record(f) = value
      }
    }
  }

  test("updateAndLock should update and lock") {
    val record = schema.newRecord()
    val value = new Object

    for (f <- fields) {
      record.updateAndLock(f, value)
      intercept[IllegalStateException] {
        record(f) = value
      }
    }
  }

  test("copy should copy") {
    val record = schema.newRecord()
    val value = new Object

    for (f <- fields) {
      record.update(f, value)
      val copy = record.copy()
      assert(record(f) eq copy(f))
    }
  }

  test("copy should not be modified when the original is updated") {
    val record = schema.newRecord()
    val copy = record.copy()

    for (f <- fields) {
      record.update(f, new Object)
      val copy = record.copy()
      record.update(f, new Object)
      assert(record(f) ne copy(f))
    }
  }

  test("locked state should be copied") {
    val record = schema.newRecord()

    for (f <- fields) {
      record.updateAndLock(f, new Object)
      intercept[IllegalStateException] {
        record.copy().update(f, new Object)
      }
    }
  }

  test("copy should be able to overwrite a locked field") {
    val record = schema.newRecord()

    for (f <- fields) {
      record.updateAndLock(f, new Object)
      val value = new Object
      val copy = record.copy(f, value)
      assert(copy(f) eq value)
    }
  }
}
