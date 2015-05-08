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

import java.util.Map;

import javax.validation.constraints.NotNull;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.SearchHit;

import com.datatorrent.api.Context.OperatorContext;

/**
 * Operator to documents from Elastic search as Map
 */
public abstract class ElasticSearchMapInputOperator<T extends Map<String, Object>> extends AbstractElasticSearchInputOperator<T, ElasticSearchConnectable>
{
  /**
   * Name of the index in elasticsearch
   */
  @NotNull
  protected String indexName;
 
  /**
   * Type of document in elasticsearch
   */
  @NotNull
  protected String documentType;

  /**
   * 
   */
  public ElasticSearchMapInputOperator()
  {
    this.store = new ElasticSearchConnectable();
  }

  /**
   * {@link SearchRequestBuilder} properties which do not change for each window are set during operator initialization.
   * 
   * @see com.datatorrent.contrib.elasticsearch.AbstractElasticSearchInputOperator#setup(com.datatorrent.api.Context.OperatorContext)
   */
  @Override
  public void setup(OperatorContext t1)
  {
    super.setup(t1);
    searchRequestBuilder.setIndices(indexName).setTypes(documentType).setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.datatorrent.contrib.elasticsearch.AbstractElasticSearchInputOperator#convertToTuple(org.elasticsearch.search
   * .SearchHit)
   */
  @Override
  protected T convertToTuple(SearchHit hit)
  {
    Map<String, Object> tuple = hit.getSource();
    return (T) tuple;
  }

  /**
   * Name of the index in elastic search
   * @param indexName
   *          the indexName to set
   */
  public void setIndexName(String indexName)
  {
    this.indexName = indexName;
  }

  /**
   * Name of the index in elastic search
   * @return the indexName
   */
  public String getIndexName()
  {
    return indexName;
  }
  
  /**
   * Type of document in elasticsearch 
   * @param documentType the documentType to set
   */
  public void setDocumentType(String documentType)
  {
    this.documentType = documentType;
  }
  
  /**
   * Type of document in elasticsearch 
   * @return the documentType
   */
  public String getDocumentType()
  {
    return documentType;
  }

}
