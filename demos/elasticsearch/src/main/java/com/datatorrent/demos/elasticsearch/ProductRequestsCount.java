/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.elasticsearch;

import com.datatorrent.lib.streamquery.function.CountFunction;

/**
 * Wrapper over CountFunction to enable default constructor
 */
public class ProductRequestsCount extends CountFunction
{

  /**
   * Defining default constructor
   */
  public ProductRequestsCount()
  {
    super(null, null);
  }

  public void setColumn(String column)
  {
    this.column = column;
  }

  public void setAlias(String alias)
  {
    this.alias = alias;
  }

}
