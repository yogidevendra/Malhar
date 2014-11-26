package com.datatorrent.contrib.solr;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrServer;

import com.datatorrent.lib.db.Connectable;

public abstract class SolrServerConnector implements Connectable
{
  protected SolrServer solrServer;

  public SolrServer getSolrServer()
  {
    return solrServer;
  }

  public void setSolrServer(SolrServer solrServer)
  {
    this.solrServer = solrServer;
  }

  @Override
  public void disconnect() throws IOException
  {
    solrServer.shutdown();
  }

  @Override
  public boolean isConnected()
  {
    throw new UnsupportedOperationException();
  }

}
