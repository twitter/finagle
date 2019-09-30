package com.twitter.finagle.mysql;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import scala.Array;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.immutable.IndexedSeq;
import scala.math.BigDecimal;
import scala.math.BigDecimal$;
import scala.math.BigInt;

import org.junit.Assert;
import org.junit.Test;

public final class RowCompilationTest {

  private static class RowImpl extends AbstractRow {
    private final List<Field> fields;
    private final List<Value> values;

    RowImpl(List<Field> fields, List<Value> values) {
      this.fields = fields;
      this.values = values;
    }

    public IndexedSeq<Field> fields() {
      return JavaConverters.asScalaBufferConverter(fields).asScala().toIndexedSeq();
    }

    public IndexedSeq<Value> values() {
      return JavaConverters.asScalaBufferConverter(values).asScala().toIndexedSeq();
    }

    public Option<Object> indexOf(String columnName) {
      int i = 0;
      while (i < fields.size()) {
        if (columnName.equals(fields.get(i).id())) {
          return Option.apply(i);
        }
        i++;
      }
      return Option.empty();
    }
  }

  /** The column name used throughout */
  private final String cn = "i";

  private Field newField() {
    return new Field(
      "catalog",
      "db",
      "table",
      "originalTable",
      cn,
      cn,
      (short) 5,
      10,
      (short) 10,
      (short) 10,
      (byte) 0
    );
  }

  private final List<Field> fields = Collections.singletonList(newField());

  private final Row intRow = new RowImpl(
      fields,
      Collections.singletonList(new ByteValue((byte) 1))
  );

  private final Row nullRow = new RowImpl(
      fields,
      Collections.singletonList(NullValue$.MODULE$)
  );

  private final Row floatRow = new RowImpl(
      fields,
      Collections.singletonList(new FloatValue(1.0f))
  );

  private final Row stringRow = new RowImpl(
      fields,
      Collections.singletonList(new StringValue("hi"))
  );

  private final Row bytesRow = new RowImpl(
      fields,
      Collections.singletonList(new RawValue(
          Type.TinyBlob(), MysqlCharset.Binary(), true, new byte[] {0, 1, 2 }))
  );

  private final Row timestampRow = new RowImpl(
      fields,
      Collections.singletonList(new RawValue(
          Type.Timestamp(), MysqlCharset.Utf8_bin(), false,
          "2018-05-05 10:20:30".getBytes(StandardCharsets.UTF_8))
      )
  );

  private final Row emptyRow = new RowImpl(
      fields,
      Collections.singletonList(EmptyValue$.MODULE$)
  );

  @Test
  public void testPrimitives() {
    boolean bool = intRow.booleanOrFalse(cn);
    Assert.assertEquals(true, bool);
    Assert.assertFalse(nullRow.booleanOrFalse(cn));

    byte b = intRow.byteOrZero(cn);
    Assert.assertEquals((byte) 1, b);
    Assert.assertEquals((byte) 0, nullRow.byteOrZero(cn));

    short s = intRow.shortOrZero(cn);
    Assert.assertEquals((short) 1, s);
    Assert.assertEquals((short) 0, nullRow.shortOrZero(cn));

    int i = intRow.intOrZero(cn);
    Assert.assertEquals(1, i);
    Assert.assertEquals(0, nullRow.intOrZero(cn));

    float f = floatRow.floatOrZero(cn);
    Assert.assertEquals(1.0f, f, 0.0);
    Assert.assertEquals(0.0f, nullRow.floatOrZero(cn), 0.0);

    double d = floatRow.doubleOrZero(cn);
    Assert.assertEquals(1.0, d, 0.0);
    Assert.assertEquals(0.0, nullRow.doubleOrZero(cn), 0.0);
  }

  @Test
  public void testGetBoxedPrimitives() {
    Option<Boolean> bool = intRow.getBoolean(cn);
    Assert.assertEquals(Boolean.TRUE, bool.get());
    Assert.assertTrue(nullRow.getBoolean(cn).isEmpty());

    Option<Byte> b = intRow.getByte(cn);
    Assert.assertEquals(Byte.valueOf((byte) 1), b.get());
    Assert.assertTrue(nullRow.getByte(cn).isEmpty());

    Option<Short> s = intRow.getShort(cn);
    Assert.assertEquals(Short.valueOf((short) 1), s.get());
    Assert.assertTrue(nullRow.getShort(cn).isEmpty());

    Option<Integer> i = intRow.getInteger(cn);
    Assert.assertEquals(Integer.valueOf(1), i.get());
    Assert.assertTrue(nullRow.getInteger(cn).isEmpty());

    Option<Long> l = intRow.getLong(cn);
    Assert.assertEquals(Long.valueOf(1), l.get());
    Assert.assertTrue(nullRow.getLong(cn).isEmpty());

    Option<Float> f = floatRow.getFloat(cn);
    Assert.assertEquals(Float.valueOf(1.0f), f.get());
    Assert.assertTrue(nullRow.getFloat(cn).isEmpty());

    Option<Double> d = floatRow.getDouble(cn);
    Assert.assertEquals(Double.valueOf(1.0), d.get());
    Assert.assertTrue(nullRow.getDouble(cn).isEmpty());
  }

  @Test
  public void testOrNulls() {
    String string = stringRow.stringOrNull(cn);
    Assert.assertEquals("hi", string);
    Assert.assertNull(nullRow.stringOrNull(cn));

    BigInt bigInt = intRow.bigIntOrNull(cn);
    Assert.assertEquals(BigInt.apply(1), bigInt);
    Assert.assertNull(nullRow.bigIntOrNull(cn));

    BigDecimal bigDecimal = floatRow.bigDecimalOrNull(cn);
    Assert.assertEquals(BigDecimal$.MODULE$.apply(1.0), bigDecimal);
    Assert.assertNull(nullRow.bigDecimalOrNull(cn));

    Date javaSqlDate = nullRow.javaSqlDateOrNull(cn);
    Assert.assertNull(javaSqlDate);

    byte[] bytes = bytesRow.bytesOrNull(cn);
    Assert.assertArrayEquals(new byte[] {0, 1, 2 }, bytes);
    Assert.assertNull(nullRow.bytesOrNull(cn));

    Timestamp timestamp = timestampRow.timestampOrNull(cn, TimeZone.getTimeZone("UTC"));
    Assert.assertEquals(1525515630000L, timestamp.getTime());
    Assert.assertNull(nullRow.timestampOrNull(cn, TimeZone.getTimeZone("UTC")));
  }

  @Test
  public void testGetOthers() {
    Option<String> string = stringRow.getString(cn);
    Assert.assertEquals("hi", string.get());
    Assert.assertTrue(nullRow.getString(cn).isEmpty());

    Option<BigInt> bigInt = intRow.getBigInt(cn);
    Assert.assertEquals(BigInt.apply(1), bigInt.get());
    Assert.assertTrue(nullRow.getBigInt(cn).isEmpty());

    Option<BigDecimal> bigDecimal = floatRow.getBigDecimal(cn);
    Assert.assertEquals(BigDecimal$.MODULE$.apply(1.0), bigDecimal.get());
    Assert.assertTrue(nullRow.getBigDecimal(cn).isEmpty());

    Option<Date> javaSqlDate = nullRow.getJavaSqlDate(cn);
    Assert.assertTrue(javaSqlDate.isEmpty());

    Option<byte[]> bytes = bytesRow.getBytes(cn);
    Assert.assertArrayEquals(new byte[] {0, 1, 2 }, bytes.get());
    Assert.assertTrue(nullRow.getBytes(cn).isEmpty());

    Option<Timestamp> timestamp = timestampRow.getTimestamp(cn, TimeZone.getTimeZone("UTC"));
    Assert.assertEquals(1525515630000L, timestamp.get().getTime());
    Assert.assertTrue(nullRow.getTimestamp(cn, TimeZone.getTimeZone("UTC")).isEmpty());
  }

  @Test
  public void testEmpty() {
    Assert.assertEquals("", emptyRow.stringOrNull(cn));
    Assert.assertEquals("", emptyRow.getString(cn).get());

    Assert.assertEquals(Array.emptyByteArray(), emptyRow.bytesOrNull(cn));
    Assert.assertEquals(Array.emptyByteArray(), emptyRow.getBytes(cn).get());
  }

}
