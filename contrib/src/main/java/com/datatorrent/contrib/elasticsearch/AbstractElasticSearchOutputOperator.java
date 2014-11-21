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

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.db.AbstractStoreOutputOperator;

/**
 * This is the base implementation for a non-transactional output operator for ElasticSearch.
 * 
 * 
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have one input port <br>
 * <b>Output</b>: no output port<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * hostName port<br>
 * 
 * <b>Benchmarks</b>: <br>
 * </p>
 * 
 * @displayName ElasticSearch Output
 * @category Store
 * @since 2.0.1
 */
public abstract class AbstractElasticSearchOutputOperator<T, S extends ElasticSearchConnectable> extends AbstractStoreOutputOperator<T, S>
{
  protected transient IndexRequestBuilder indexRequestBuilder;

  /**
   * Initialize transient fields such as {@code IndexRequestBuilder}
   * 
   * @see com.datatorrent.lib.db.AbstractStoreOutputOperator#setup(com.datatorrent.api.Context.OperatorContext)
   */
  @Override
  public void setup(OperatorContext context)
  {

    super.setup(context);
    indexRequestBuilder = new IndexRequestBuilder(store.client);
  }

  /**
   * Calls {@link #indexTuple(T)} to add a document into ElasticSearch
   * 
   * @see com.datatorrent.lib.db.AbstractStoreOutputOperator#processTuple(java.lang.Object)
   */
  public void processTuple(T tuple)
  {
    indexTuple(tuple);
  }

  /**
   * One document is added to ElasticSearch per tuple. {@code getId()} is called to generate the id field.<br>
   * If document with given id already exists; then it will get overwritten.<br>
   * If {@code getId()} returns {@code null} then document will be added to elastic search without id. In this case
   * ElasticSearch will add auto-generated _id field to the document.<br>
   * 
   * @param tuple
   * @return
   */
  protected IndexResponse indexTuple(T tuple)
  {
    String id = getId(tuple);
    if (id != null) {
      indexRequestBuilder.setId(id);
    }

    return getIndexRequestBuilder(tuple).execute().actionGet();
  }

  /**
   * Convert tuple into format which can be set as source into {@link IndexRequestBuilder} such as Map, String, byte[]
   * etc. Set tuple specific properties on {@link IndexRequestBuilder}.
   * 
   * @param tuple
   * @return
   */
  protected abstract IndexRequestBuilder getIndexRequestBuilder(T tuple);

  /**
   * Determine id for the given tuple.<br>
   * If tuples do not have any field mapping to unique id then this function should return null. In this case
   * ElasticSearch will add auto-generated _id field to the document. {@code ProcessingMode.EXACTLY_ONCE} is supported
   * only if getId() returns unique value for each tuple.
   * 
   * @param tuple
   * @return
   */
  protected abstract String getId(T tuple);
}
