package com.twitter.finagle.mysql;

import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import com.twitter.util.Future;

public final class PreparedStatementCompilationTest {

  private PreparedStatement.AsJava preparedStatement() {
    PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
    Mockito.when(stmt.apply(ArgumentMatchers.any()))
        .thenReturn(Future.exception(new RuntimeException("nope")));
    Mockito.when(stmt.select(ArgumentMatchers.any(), ArgumentMatchers.any()))
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
  public void testModify() {
    PreparedStatement.AsJava stmt = preparedStatement();
    stmt.modify(Parameters.nullParameter());
    stmt.modify(
        Parameters.nullParameter(),
        Parameters.of("hello")
    );
  }

  @Test
  public void testRead() {
    PreparedStatement.AsJava stmt = preparedStatement();
    stmt.read(Parameters.nullParameter());
    stmt.read(
        Parameters.nullParameter(),
        Parameters.of("hello")
    );
  }

  @Test
  public void testSelect() {
    PreparedStatement.AsJava stmt = preparedStatement();
    stmt.select(row -> "a row", Parameters.nullParameter());
    stmt.select(row -> row.stringOrNull("columnName"),
      Parameters.nullParameter(),
      Parameters.of(22)
    );
  }

}
