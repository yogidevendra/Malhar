/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.elasticsearch;

import java.io.IOException;

import javax.validation.constraints.NotNull;

import org.elasticsearch.action.percolate.PercolateResponse;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.DTThrowable;

/**
 * Percolate operator for ElasticSearch
 * 
 */
public class ElasticSearchPercolatorOperator extends BaseOperator
{
  /**
   * Host name of the elastic search server to connect
   */
  @NotNull
  protected String hostName;
  /**
   * Port number to connect on elastic search server
   */
  protected int port;
  /**
   * Name of the index in elastic search
   */
  @NotNull
  protected String indexName;
  /**
   * Type of document in elasticsearch
   */
  @NotNull
  protected String documentType;

  protected transient ElasticSearchPercolatorStore store;
  public final transient DefaultOutputPort<PercolateResponse> outputPort = new DefaultOutputPort<PercolateResponse>();

  public final transient DefaultInputPort<Object> inputPort = new DefaultInputPort<Object>() {

    /*
     * (non-Javadoc)
     * 
     * @see com.datatorrent.api.DefaultInputPort#process(java.lang.Object)
     */
    @Override
    public void process(Object tuple)
    {

      PercolateResponse response = store.percolate(new String[] { indexName }, documentType, tuple);
      outputPort.emit(response);
    }
  };

  @Override
  public void setup(com.datatorrent.api.Context.OperatorContext context)
  {
    store = new ElasticSearchPercolatorStore(hostName, port);
    try {
      store.connect();
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }
  
  /* (non-Javadoc)
   * @see com.datatorrent.api.BaseOperator#teardown()
   */
  @Override
  public void teardown()
  {
    super.teardown();
    try {
      store.disconnect();
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }
  
  /**
   * Host name of the elastic search server to connect
   * @return the hostName
   */
  public String getHostName()
  {
    return hostName;
  }
  
  /**
   * Host name of the elastic search server to connect
   * @param hostName the hostName to set
   */
  public void setHostName(String hostName)
  {
    this.hostName = hostName;
  }
  
  
  
  /**
   * @return the port
   */
  public int getPort()
  {
    return port;
  }
  
  /**
   * @param port the port to set
   */
  public void setPort(int port)
  {
    this.port = port;
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
   * Name of the index in elastic search
   * @param indexName the indexName to set
   */
  public void setIndexName(String indexName)
  {
    this.indexName = indexName;
  }
  
  /**
   * Type of document in elasticsearch
   * @return the documentType
   */
  public String getDocumentType()
  {
    return documentType;
  }
  
  /**
   * Type of document in elasticsearch
   * @param documentType the documentType to set
   */
  public void setDocumentType(String documentType)
  {
    this.documentType = documentType;
  }
  
}
