package com.datatorrent.demos.solr;

import java.util.Random;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

public class InputItemGenerator implements InputOperator
{
  enum QueryType {
    SEARCH, PURCHASE, WATCH
  }

  private static final int CATAGORY_COUNT = 50;
  private static final int BLAST_COUNT = 1000;
  private final Random random = new Random();
  @OutputPortFieldAnnotation
  public final transient DefaultOutputPort<ProductInfo> outputPort = new DefaultOutputPort<ProductInfo>();

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void emitTuples()
  {
    for (int i = 0; i < BLAST_COUNT; ++i) {
      int categoryId = nextRandomId(CATAGORY_COUNT);
      int productId = nextRandomId(Integer.MAX_VALUE);
      String queryType = getQueryType(i);
      emitTuple(categoryId, productId, queryType);
    }

  }

  private void emitTuple(int categoryId, int productId, String queryType)
  {
    outputPort.emit(new ProductInfo(categoryId, productId, queryType));
  }

  private String getQueryType(int i)
  {
    if (i % 3 == 0)
      return QueryType.SEARCH.toString();
    else if (i % 3 == 1)
      return QueryType.PURCHASE.toString();
    else
      return QueryType.WATCH.toString();
  }

  private int nextRandomId(int max)
  {
    return random.nextInt(max);
  }

}
