/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.elasticsearch;

import java.util.Map;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.FilterBuilders;

import com.datatorrent.contrib.elasticsearch.ElasticSearchMapInputOperator;

/**
 * Query operator to fetch last 10 minutes of data
 */
public class ElasticSearchProductQueryOperator<T extends Map<String, Object>> extends ElasticSearchMapInputOperator<Map<String, Object>>
{

  /*
   * (non-Javadoc)
   * 
   * @see com.datatorrent.contrib.elasticsearch.AbstractElasticSearchInputOperator#getSearchRequestBuilder()
   */

  @Override
  protected SearchRequestBuilder getSearchRequestBuilder()
  {
    long time = System.currentTimeMillis();
    // Fetch records from last one minute
    return searchRequestBuilder.setPostFilter(FilterBuilders.rangeFilter(ElasticSearchDemoApp.TIMESTAMP).from(time - ElasticSearchDemoApp.MILISEC_10MIN).to(time)) // Filter
        .setExplain(false);
  }

}
