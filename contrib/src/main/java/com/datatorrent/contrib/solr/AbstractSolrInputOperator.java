package com.datatorrent.contrib.solr;

import java.io.IOException;

import javax.validation.constraints.NotNull;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * This is a base implementation of a Solr input operator, which consumes data from solr search operations.&nbsp;
 * Subclasses should implement the methods initializeSolrServerConnector, getQueryParams, emitTuple for emitting tuples
 * to downstream operators.
 * <p>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method initializeSolrServerConnector(), getQueryParams() and
 * emitTuple() <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 *
 * Shipped jars with this operator:<br>
 * <b>org.apache.solr.client.solrj.SolrServer.class</b> Solrj - Solr Java Client <br>
 * <br>
 * </p>
 *
 * @displayName Abstract Solr Input
 * @category Search Engine
 * @tags input operator
 *
 * @since 2.0.0
 */

public abstract class AbstractSolrInputOperator<T> implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractSolrInputOperator.class);
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();
  @NotNull
  protected SolrServerConnector solrServerConnector;
  private SolrDocument lastEmittedTuple;
  private long lastEmittedTimeStamp;

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      initializeSolrServerConnector();
      solrServerConnector.connect();
    } catch (IOException ex) {
      throw new RuntimeException("Unable to connect to Solr Server", ex);
    }
  }

  @Override
  public void teardown()
  {
    solrServerConnector.getSolrServer().shutdown();
  }

  public long getLastEmittedTimeStamp()
  {
    return lastEmittedTimeStamp;
  }

  public SolrDocument getLastEmittedTuple()
  {
    return lastEmittedTuple;
  }

  public SolrServer getSolrServer()
  {
    return solrServerConnector.getSolrServer();
  }

  public SolrServerConnector getSolrServerConnector()
  {
    return solrServerConnector;
  }

  public void setSolrServerConnector(SolrServerConnector solrServerConnector)
  {
    this.solrServerConnector = solrServerConnector;
  }

  @Override
  public void emitTuples()
  {
    SolrParams solrQueryParams = getQueryParams();
    try {
      SolrServer solrServer = solrServerConnector.getSolrServer();
      QueryResponse response = solrServer.query(solrQueryParams);
      SolrDocumentList queriedDocuments = response.getResults();
      for (SolrDocument solrDocument : queriedDocuments) {
        emitTuple(solrDocument);
        lastEmittedTuple = solrDocument;
        lastEmittedTimeStamp = System.currentTimeMillis();

        logger.debug("Emiting document: " + solrDocument.getFieldValue("name"));
      }
    } catch (SolrServerException ex) {
      throw new RuntimeException("Unable to fetch documents from Solr server", ex);
    }
  }

  /**
   * Initialize SolrServer object
   */
  public abstract void initializeSolrServerConnector();

  protected abstract void emitTuple(SolrDocument document);

  /**
   * Any concrete class has to override this method to return the query string which will be used to retrieve documents
   * from Solr server.
   *
   * @return Query string
   */
  public abstract SolrParams getQueryParams();

}
