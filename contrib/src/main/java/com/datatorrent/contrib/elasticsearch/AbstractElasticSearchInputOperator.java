/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.*/

package com.datatorrent.contrib.elasticsearch;

import javax.validation.constraints.Min;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.db.AbstractStoreInputOperator;

/**
 * This is the base implementation for a non transactional input operator for ElasticSearch
 * 
 * <br>
 * Ports:<br>
 * <b>Input</b>: no input port<br>
 * <b>Output</b>: You can have one output port<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * hostName port<br>
 * 
 * <b>Benchmarks</b>: <br>
 * 
 * @displayName ElasticSearch input
 * @category Store
 * @tags input operator
 */
public abstract class AbstractElasticSearchInputOperator<T, S extends ElasticSearchConnectable> extends AbstractStoreInputOperator<T, S>
{
  protected transient SearchRequestBuilder searchRequestBuilder;

  /**
   * Initializing transient fields such as ElasticSearchConnectable, SearchRequestBuilder
   * 
   * @see com.datatorrent.lib.db.AbstractStoreInputOperator#setup(com.datatorrent.api.Context.OperatorContext)
   */
  @Override
  public void setup(OperatorContext t1)
  {
    super.setup(t1);
    this.searchRequestBuilder = new SearchRequestBuilder(store.client);
  }

  /**
   * Emit one tuple per {@code SearchHit} for given search query
   * 
   * @see com.datatorrent.api.InputOperator#emitTuples()
   */
  @Override
  public void emitTuples()
  {
    SearchResponse response = getSearchRequestBuilder().execute().actionGet();
    for (SearchHit hit : response.getHits().hits()) {
      outputPort.emit(convertToTuple(hit));
    }
  }

  /**
   * Converts SearchHit to Tuple
   * 
   * @param hit
   * @return tuple constructed from <code>hit</code>
   */
  protected abstract T convertToTuple(SearchHit hit);

  /**
   * Set {@link SearchRequestBuilder} properties according to search query requirements. Properties which do not change
   * for each window can be set in {@code setup()}. Properties which may change for each window should be set in this
   * function
   * 
   * @return {@link SearchRequestBuilder}
   */
  protected abstract SearchRequestBuilder getSearchRequestBuilder();

}
