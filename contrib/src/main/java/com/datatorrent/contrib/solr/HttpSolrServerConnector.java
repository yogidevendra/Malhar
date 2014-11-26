package com.datatorrent.contrib.solr;

import org.apache.solr.client.solrj.impl.HttpSolrServer;

public class HttpSolrServerConnector extends SolrServerConnector
{
  private static final String DEFAULT_SOLR_SERVER_URL = "http://localhost:8983/solr";
  private String solrServerURL = DEFAULT_SOLR_SERVER_URL;

  @Override
  public void connect()
  {
    solrServer = new HttpSolrServer(solrServerURL);
  }

  // set this property in dt-site.xml
  public void setSolrServerURL(String solrServerURL)
  {
    this.solrServerURL = solrServerURL;
  }

  public String getSolrServerURL()
  {
    return solrServerURL;
  }

}
