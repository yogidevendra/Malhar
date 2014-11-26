package com.datatorrent.demos.solr;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.solr.HttpSolrServerConnector;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name = "SolrInputOperatorDemo")
public class SolrSearchApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    SolrInputOperator inputOpr = dag.addOperator("solrReader", SolrInputOperator.class);
    inputOpr.setSolrServerConnector(new HttpSolrServerConnector());
    SolrSearchAggregator aggregator = dag.addOperator("aggregator", new SolrSearchAggregator());
    ConsoleOutputOperator outputOpr = dag.addOperator("console", new ConsoleOutputOperator());

    dag.addStream("solrInputStream", inputOpr.outputPort, aggregator.input).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("consoleStream", aggregator.output, outputOpr.input).setLocality(Locality.CONTAINER_LOCAL);

  }

}
