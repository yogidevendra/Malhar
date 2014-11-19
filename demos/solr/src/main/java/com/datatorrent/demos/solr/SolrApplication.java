package com.datatorrent.demos.solr;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.solr.HttpSolrServerConnector;

@ApplicationAnnotation(name = "SolrOutputOperatorDemo")
public class SolrApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    InputItemGenerator inputOpr = dag.addOperator("InputGenerator", new InputItemGenerator());
    SolrOutputOperator outputOpr = dag.addOperator("SolrOutputOperator", new SolrOutputOperator());
    outputOpr.setSolrServerConnector(new HttpSolrServerConnector());

    dag.addStream("prodInfoStream", inputOpr.outputPort, outputOpr.inputPort).setLocality(Locality.CONTAINER_LOCAL);

  }

}
