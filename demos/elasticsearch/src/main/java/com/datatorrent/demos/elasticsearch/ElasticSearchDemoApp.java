/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.elasticsearch;

import com.datatorrent.api.StreamingApplication;

/**
 * Common functions for ElasticSearch DemoApplication go here
 */
public abstract class ElasticSearchDemoApp implements StreamingApplication
{
  /**
   * Index name under which tuple should be stored ElasticSearch requires index name to be in small case
   */
  // static final String INDEX_NAME = "product_requests";
  /**
   * Data Type for records/documents
   */
  // static final String TYPE = "purchase";

  // Fields to be populated for each record.
  static final String PRODUCT_NAME = "ProductName";
  static final String TIMESTAMP = "timestamp";
  static final String REQUEST_ID = "RequestId";

  static final long MILISEC_10MIN = 10 * 60 * 1000;

}
