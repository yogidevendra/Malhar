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

import java.io.IOException;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.DTThrowable;

/**
 * Concrete implementation of {@link AbstractElasticSearchOutputOperator} demonstrating the functionality for Tuples of
 * Map type.
 * 
 */
public class ElasticSearchMapOutputOperator<T extends Map<String, Object>> extends AbstractElasticSearchOutputOperator<T, ElasticSearchConnectable>
{
  private static final Logger logger = LoggerFactory.getLogger(ElasticSearchMapOutputOperator.class);
  private String idField;
  protected String indexName;
  protected String type;

  private transient ObjectMapper mapper;

  /**
   * 
   */
  public ElasticSearchMapOutputOperator()
  {
    this.store = new ElasticSearchConnectable();
  }

  /*
   * 
   * (non-Javadoc)
   * 
   * @see com.datatorrent.contrib.elasticsearch.AbstractElasticSearchOutputOperator#setup(com.datatorrent.api.Context.
   * OperatorContext)
   */
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    mapper = new ObjectMapper();
    indexRequestBuilder.setIndex(indexName);
    indexRequestBuilder.setType(type);
  }

  /**
   * 
   * @see com.datatorrent.contrib.elasticsearch.AbstractElasticSearchOutputOperator#getIndexRequestBuilder(java.lang.Object)
   */
  @Override
  protected IndexRequestBuilder getIndexRequestBuilder(T tuple)
  {
    return indexRequestBuilder.setSource(tuple);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.datatorrent.contrib.elasticsearch.AbstractElasticSearchOutputOperator#getId(java.lang.Object)
   */
  @Override
  protected String getId(T tuple)
  {
    if (idField != null) {
      return null;
    } else {
      return tuple.get(idField).toString();
    }

  }

  /**
   * @param indexName
   *          the indexName to set
   */
  public void setIndexName(String indexName)
  {
    this.indexName = indexName;
  }

  /**
   * @return the indexName
   */
  public String getIndexName()
  {
    return indexName;
  }

  /**
   * @param type
   *          the type to set
   */
  public void setType(String type)
  {
    this.type = type;
  }

  /**
   * @return the type
   */
  public String getType()
  {
    return type;
  }

  /**
   * @param idField
   *          the idField to set
   */
  public void setIdField(String idField)
  {
    this.idField = idField;
  }

  /**
   * @return the idField
   */
  public String getIdField()
  {
    return idField;
  }

}
