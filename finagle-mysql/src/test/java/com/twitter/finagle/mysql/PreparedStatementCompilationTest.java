package com.twitter.finagle.mysql;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.twitter.util.Future;

public final class PreparedStatementCompilationTest {

  private PreparedStatement.AsJava preparedStatement() {
    PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
    Mockito.when(stmt.apply(Matchers.any()))
        .thenReturn(Future.exception(new RuntimeException("nope")));
    Mockito.when(stmt.select(Matchers.any(), Matchers.any()))
        .thenReturn(Future.exception(new RuntimeException("nope")));
    return new PreparedStatement.AsJava(stmt);
  }

  @Test
  public void testExecute() {
    PreparedStatement.AsJava stmt = preparedStatement();
    stmt.execute(Parameters.nullParameter());
    stmt.execute(
        Parameters.nullParameter(),
        Parameters.of("hello")
    );
  }

  @Test
  public void testSelect() {
    PreparedStatement.AsJava stmt = preparedStatement();
    stmt.select(row -> "a row", Parameters.nullParameter());
    stmt.select((Row row) -> {
        // todo: create a Java-friendly Row API that can be used here (and in the AsJava example)
        StringValue sv = (StringValue) row.apply("columnName").get();
        return sv.s();
      },
      Parameters.nullParameter(),
      Parameters.of(22)
    );
  }

}
