/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.elasticsearch;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * For each tuple on input port generate product purchase request on output port.
 */
public class ProductTuplesGenerator<T> extends BaseOperator
{
  public final transient DefaultOutputPort<Map<String, Object>> outputPort = new DefaultOutputPort<Map<String, Object>>();

  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>() {

    /*
     * (non-Javadoc)
     * 
     * @see com.datatorrent.api.DefaultInputPort#process(java.lang.Object)
     */
    @Override
    public void process(T tuple)
    {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put(ElasticSearchOutputOperatorDemoApp.PRODUCT_NAME, tuple.toString());
      map.put(ElasticSearchOutputOperatorDemoApp.TIMESTAMP, System.currentTimeMillis());
      map.put(ElasticSearchOutputOperatorDemoApp.REQUEST_ID, UUID.randomUUID().toString());
      outputPort.emit(map);
    }
  };
}
