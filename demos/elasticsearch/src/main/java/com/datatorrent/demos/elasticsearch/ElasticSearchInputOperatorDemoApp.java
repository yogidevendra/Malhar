/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.elasticsearch;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.elasticsearch.ElasticSearchMapInputOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.streamquery.GroupByHavingOperator;
import com.datatorrent.lib.streamquery.index.ColumnIndex;

/**
 * Application to demonstrate Elastic search input operator. <br>
 * <br>
 * This demonstrates usecase for e-commerce domain.<br>
 * Program will query for records from given index in the ElasticSearch for last ten minute using Datatorrent
 * ElasticSearch Input Operator.<br>
 * commented: Results are then grouped on the product name attribute to generate the counts.<br>
 * commented: Counts for each product name in last ten minute sent as output to console.<br>
 * 
 */

@ApplicationAnnotation(name = "ElasticSearchInputOperatorDemo")
public class ElasticSearchInputOperatorDemoApp extends ElasticSearchDemoApp
{

  @Override
  public void populateDAG(DAG dag, Configuration entries)
  {
    ElasticSearchProductQueryOperator<Map<String, Object>> esInputOperator = new ElasticSearchProductQueryOperator<Map<String, Object>>();

    // GroupByHavingOperator groupByOperator = new GroupByHavingOperator();
    // groupByOperator.addColumnGroupByIndex(new ColumnIndex(PRODUCT_NAME, null));
    // ProductRequestsCount productRequestsCount = new ProductRequestsCount();
    // productRequestsCount.setColumn(REQUEST_ID);
    // groupByOperator.addAggregateIndex(productRequestsCount);

    ConsoleOutputOperator consoleOutOperator = new ConsoleOutputOperator();

    // Add Operators to Application DAG
    ElasticSearchMapInputOperator<Map<String, Object>> esInputOp = dag.addOperator("ElasticSearchInputQuery", esInputOperator);
    // GroupByHavingOperator groupByOp = dag.addOperator("GroupByProductName", groupByOperator);
    ConsoleOutputOperator consoleOutOp = dag.addOperator("Console", consoleOutOperator);

    // Connect the Operators using Streams
    // dag.addStream("ProductRequestInLast1Min", esInputOp.outputPort, groupByOp.inport);
    dag.addStream("ProductRequestInLast1Min", esInputOp.outputPort, consoleOutOp.input);
    // dag.addStream("ProductsRequestsSummary", groupByOp.outport, consoleOutOp.input);

  }

}
