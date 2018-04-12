package com.twitter.finagle.mysql;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.twitter.util.Future;

public class CursoredStatementCompilationTest {

  private CursoredStatement.AsJava cursoredStatement() {
    CursoredStatement stmt = Mockito.mock(CursoredStatement.class);
    Mockito.when(stmt.apply(
        Matchers.anyInt(),
        Matchers.any(),
        Matchers.any())
    ).thenReturn(Future.exception(new RuntimeException("nope")));
    return new CursoredStatement.AsJava(stmt);
  }

  @Test
  public void testExecute() {
    CursoredStatement.AsJava stmt = cursoredStatement();
    int rowsToFetch = 10;
    Future<CursorResult<Integer>> result = stmt.execute(
        rowsToFetch,
        (Row row) -> row.intOrZero("columnName"),
        Parameters.of(22)
    );
  }
}
