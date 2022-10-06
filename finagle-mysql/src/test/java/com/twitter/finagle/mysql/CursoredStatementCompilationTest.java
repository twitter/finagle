package com.twitter.finagle.mysql;

import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import com.twitter.util.Future;

public class CursoredStatementCompilationTest {

  private CursoredStatement.AsJava cursoredStatement() {
    CursoredStatement stmt = Mockito.mock(CursoredStatement.class);
    Mockito.when(stmt.apply(
        ArgumentMatchers.anyInt(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any())
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
