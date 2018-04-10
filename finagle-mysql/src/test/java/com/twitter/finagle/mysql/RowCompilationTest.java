package com.twitter.finagle.mysql;

import java.sql.Date;
import java.util.Collections;
import java.util.List;

import scala.Option;
import scala.collection.IndexedSeq;
import scala.collection.JavaConversions;
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
      return JavaConversions.asScalaBuffer(fields).toIndexedSeq();
    }

    public IndexedSeq<Value> values() {
      return JavaConversions.asScalaBuffer(values).toIndexedSeq();
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

  private Field newField(String name) {
    return new Field(
      "catalog",
      "db",
      "table",
      "originalTable",
      name,
      name,
      (short) 5,
      10,
      (short) 10,
      (short) 10,
      (byte) 0
    );
  }


  private final Row intRow = new RowImpl(
      Collections.singletonList(newField("i")),
      Collections.singletonList(new ByteValue((byte) 1))
  );

  private final Row nullRow = new RowImpl(
      Collections.singletonList(newField("i")),
      Collections.singletonList(NullValue$.MODULE$)
  );

  private final Row floatRow = new RowImpl(
      Collections.singletonList(newField("i")),
      Collections.singletonList(new FloatValue(1.0f))
  );

  private final Row stringRow = new RowImpl(
      Collections.singletonList(newField("i")),
      Collections.singletonList(new StringValue("hi"))
  );

  @Test
  public void testPrimitives() {
    byte b = intRow.byteOrZero("i");
    Assert.assertEquals((byte) 1, b);
    Assert.assertEquals((byte) 0, nullRow.byteOrZero("i"));

    short s = intRow.shortOrZero("i");
    Assert.assertEquals((short) 1, s);
    Assert.assertEquals((short) 0, nullRow.shortOrZero("i"));

    int i = intRow.intOrZero("i");
    Assert.assertEquals(1, i);
    Assert.assertEquals(0, nullRow.intOrZero("i"));

    float f = floatRow.floatOrZero("i");
    Assert.assertEquals(1.0f, f, 0.0);
    Assert.assertEquals(0.0f, nullRow.floatOrZero("i"), 0.0);

    double d = floatRow.doubleOrZero("i");
    Assert.assertEquals(1.0, d, 0.0);
    Assert.assertEquals(0.0, nullRow.doubleOrZero("i"), 0.0);
  }

  @Test
  public void testGetBoxedPrimitives() {
    Option<Byte> b = intRow.getByte("i");
    Assert.assertEquals(Byte.valueOf((byte) 1), b.get());
    Assert.assertTrue(nullRow.getByte("i").isEmpty());

    Option<Short> s = intRow.getShort("i");
    Assert.assertEquals(Short.valueOf((short) 1), s.get());
    Assert.assertTrue(nullRow.getShort("i").isEmpty());

    Option<Integer> i = intRow.getInteger("i");
    Assert.assertEquals(Integer.valueOf(1), i.get());
    Assert.assertTrue(nullRow.getInteger("i").isEmpty());

    Option<Long> l = intRow.getLong("i");
    Assert.assertEquals(Long.valueOf(1), l.get());
    Assert.assertTrue(nullRow.getLong("i").isEmpty());

    Option<Float> f = floatRow.getFloat("i");
    Assert.assertEquals(Float.valueOf(1.0f), f.get());
    Assert.assertTrue(nullRow.getFloat("i").isEmpty());

    Option<Double> d = floatRow.getDouble("i");
    Assert.assertEquals(Double.valueOf(1.0), d.get());
    Assert.assertTrue(nullRow.getDouble("i").isEmpty());
  }

  @Test
  public void testOrNulls() {
    String string = stringRow.stringOrNull("i");
    Assert.assertEquals("hi", string);
    Assert.assertNull(nullRow.stringOrNull("i"));

    BigInt bigInt = intRow.bigIntOrNull("i");
    Assert.assertEquals(BigInt.apply(1), bigInt);
    Assert.assertNull(nullRow.bigIntOrNull("i"));

    BigDecimal bigDecimal = floatRow.bigDecimalOrNull("i");
    Assert.assertEquals(BigDecimal$.MODULE$.apply(1.0), bigDecimal);
    Assert.assertNull(nullRow.bigDecimalOrNull("i"));

    Date javaSqlDate = nullRow.javaSqlDateOrNull("i");
    Assert.assertNull(javaSqlDate);
  }

  @Test
  public void testGetOthers() {
    Option<String> string = stringRow.getString("i");
    Assert.assertEquals("hi", string.get());
    Assert.assertTrue(nullRow.getString("i").isEmpty());

    Option<BigInt> bigInt = intRow.getBigInt("i");
    Assert.assertEquals(BigInt.apply(1), bigInt.get());
    Assert.assertTrue(nullRow.getBigInt("i").isEmpty());

    Option<BigDecimal> bigDecimal = floatRow.getBigDecimal("i");
    Assert.assertEquals(BigDecimal$.MODULE$.apply(1.0), bigDecimal.get());
    Assert.assertTrue(nullRow.getBigDecimal("i").isEmpty());

    Option<Date> javaSqlDate = nullRow.getJavaSqlDate("i");
    Assert.assertTrue(javaSqlDate.isEmpty());
  }

}
