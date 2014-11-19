package com.datatorrent.demos.solr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.demos.solr.InputItemGenerator.QueryType;

public class SolrSearchAggregator extends BaseOperator
{

  private static final Integer TOP_5 = 5;
  private Map<Integer, List<ProductInfo>> queryData = new HashMap<Integer, List<ProductInfo>>();
  private Map<Integer, Integer> categorySearchCount = new HashMap<Integer, Integer>();

  public final transient DefaultInputPort<ProductInfo> input = new DefaultInputPort<ProductInfo>() {

    @Override
    public void process(ProductInfo tuple)
    {
      int categoryId = tuple.getCategoryId();
      List<ProductInfo> productInfoList = queryData.get(categoryId);
      if (productInfoList == null) {
        productInfoList = new ArrayList<ProductInfo>();
        queryData.put(categoryId, productInfoList);
      }
      productInfoList.add(tuple);
      categorySearchCount.put(categoryId, productInfoList.size());
    }
  };

  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  @Override
  public void endWindow()
  {
    List<Map.Entry<Integer, Integer>> sortedEntries = sortEntries();
    for (int i = 0; i < (sortedEntries.size() > TOP_5 ? TOP_5 : sortedEntries.size()); i++) {
      List<ProductInfo> prodList = queryData.get(sortedEntries.get(i).getKey());
      Map<String, AtomicInteger> queryCounters = getQueryCounters(prodList);
      output.emit("\nCategoryId: " + sortedEntries.get(i).getKey() + " Hit Count:" + prodList.size() + "\nPurchase Frequency: " + queryCounters.get(QueryType.PURCHASE.toString()) + "\nSearch Frequency: " + queryCounters.get(QueryType.SEARCH.toString()) + "\nWatch Frequency: " + queryCounters.get(QueryType.WATCH.toString()));
    }
    queryData.clear();
    categorySearchCount.clear();
  }

  private Map<String, AtomicInteger> getQueryCounters(List<ProductInfo> prodList)
  {
    Map<String, AtomicInteger> queryCounters = new HashMap<String, AtomicInteger>();
    queryCounters.put(QueryType.PURCHASE.toString(), new AtomicInteger(0));
    queryCounters.put(QueryType.SEARCH.toString(), new AtomicInteger(0));
    queryCounters.put(QueryType.WATCH.toString(), new AtomicInteger(0));
    for (ProductInfo product : prodList) {
      queryCounters.get(product.getQueryType()).incrementAndGet();
    }
    return queryCounters;

  }

  private List<Map.Entry<Integer, Integer>> sortEntries()
  {
    List<Map.Entry<Integer, Integer>> sortedEntries = new LinkedList<Map.Entry<Integer, Integer>>(categorySearchCount.entrySet());

    Collections.sort(sortedEntries, new Comparator<Map.Entry<Integer, Integer>>() {
      public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2)
      {
        return (o2.getValue()).compareTo(o1.getValue());
      }
    });
    return sortedEntries;
  }
}
