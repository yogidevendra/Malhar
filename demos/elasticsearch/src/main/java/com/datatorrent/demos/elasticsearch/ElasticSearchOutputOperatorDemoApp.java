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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.elasticsearch.ElasticSearchMapOutputOperator;
import com.datatorrent.lib.testbench.WeightBiasRandomInputOperator;

/**
 * Application to demonstrate Elastic search output operator. <br>
 * <br>
 * This demonstrates usecase for e-commerce domain.<br>
 * Program will generate product purchase requests by picking random products from given list of product names (named as
 * "A" to "Z" for this example) based on relative weight bias assigned to them.<br>
 * These requests are then converted to Map by adding additional annotations such as timestamp and unique ID for each
 * request.<br>
 * <br>
 * These tuples are then written to ElasticSearch for indexing.<br>
 * Data indexed by ElasticSearch can be read by any client for ElasticSearch. For example, client could be kibana, http
 * requests, DataTorrent ElasticSearchInputOperator. <br>
 * Using DataTorrent ElasticSearchInputOperator this data can be fed to other applications running on DataTorrent
 * platform.
 */
@ApplicationAnnotation(name = "ElasticSearchOutputOperatorDemo")
public class ElasticSearchOutputOperatorDemoApp extends ElasticSearchDemoApp
{

  @Override
  public void populateDAG(DAG dag, Configuration entries)
  {
    // Generate product names
    List<String> products = populateProductNames();
    // Generate bias for the products
    int[] weights = populateWeights(products);

    // Operator to pick random products from given list of products
    WeightBiasRandomInputOperator<String> productsGenerator = new WeightBiasRandomInputOperator<String>();
    productsGenerator.setItems(products.toArray(new String[0]));
    productsGenerator.setWeights(weights);

    // Operator to generate tuples for product requests
    ProductTuplesGenerator<String> productTuplesGenerator = new ProductTuplesGenerator<String>();

    // ElasticSearch Output Operator will write tuples to the elastic search
    ElasticSearchMapOutputOperator<Map<String, Object>> esOutPut = new ElasticSearchMapOutputOperator<Map<String, Object>>();
    // Set properties for ElasticSearch Output operator

    // Add Operators to Application DAG
    WeightBiasRandomInputOperator<String> productGen = dag.addOperator("RandomProductsPicker", productsGenerator);
    ProductTuplesGenerator<String> productRequestGen = dag.addOperator("ProductRequestGenerator", productTuplesGenerator);
    ElasticSearchMapOutputOperator<Map<String, Object>> esOutOperator = dag.addOperator("ElasticSearchIndexWriter", esOutPut);

    // Connect the Operators using Streams
    dag.addStream("ProductsPicked", productGen.outPort, productRequestGen.inputPort);
    dag.addStream("ProductsRequests", productRequestGen.outputPort, esOutOperator.input);

  }

  private List<String> populateProductNames()
  {
    List<String> products = new ArrayList<String>();
    for (char ch = 'A'; ch <= 'Z'; ch++) {
      String product = ch + "";
      products.add(product);
    }
    return products;
  }

  private int[] populateWeights(List<String> products)
  {
    // Initialize product weights
    int[] weights = new int[products.size()];
    for (int i = 0; i < weights.length; i++) {
      weights[i] = 1;
    }
    weights[products.indexOf("P")] = 10;
    weights[products.indexOf("X")] = 20;
    return weights;
  }

}
