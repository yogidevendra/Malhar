package com.datatorrent.contrib.solr;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import javax.validation.constraints.NotNull;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

/**
 * This is a base implementation of a Solr output operator, which adds data to Solr Server.&nbsp; Subclasses should
 * implement the methods initializeSolrServerConnector and getTuple.
 * <p>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method initializeSolrServerConnector() and getTuple() <br>
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
 * @displayName Abstract Solr Output
 * @category Search Engine
 * @tags output operator
 *
 * @since 2.0.0
 */
public abstract class AbstractSolrOutputOperator<T> implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractSolrOutputOperator.class);
  @NotNull
  protected SolrServerConnector solrServerConnector;
  private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
  private static int HOLDING_BUFFER_SIZE = DEFAULT_BUFFER_SIZE;
  private transient Queue<SolrInputDocument> docBuffer;
  private transient long lastProcessedWindow;
  private transient long currentWindow;
  @InputPortFieldAnnotation
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>() {
    @Override
    public void process(T tuple)
    {
      if (currentWindow > lastProcessedWindow) {
        if (docBuffer.size() >= HOLDING_BUFFER_SIZE) {
          processTuples();
        }
        SolrInputDocument solrDocument = convertTuple(tuple);
        if (solrDocument != null) {
          docBuffer.add(solrDocument);
        }
      }
    }
  };

  /**
   * Initialize SolrServer object
   */
  public abstract void initializeSolrServerConnector();

  /**
   * Converts the object into Solr document format
   * 
   * @param object
   *          to be stored to Solr Server
   * @return
   */
  public abstract SolrInputDocument convertTuple(T tuple);

  @Override
  public void setup(OperatorContext context)
  {
    lastProcessedWindow = -1;
    currentWindow = 0;
    docBuffer = new ArrayBlockingQueue<SolrInputDocument>(HOLDING_BUFFER_SIZE);
    try {
      initializeSolrServerConnector();
      solrServerConnector.connect();
    } catch (Exception ex) {
      throw new RuntimeException("Unable to connect to Solr server", ex);
    }
  }

  @Override
  public void teardown()
  {
    docBuffer.clear();
    solrServerConnector.getSolrServer().shutdown();
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindow = windowId;
  }

  @Override
  public void endWindow()
  {
    processTuples();
    lastProcessedWindow = currentWindow;
  }

  private void processTuples()
  {
    try {
      SolrServer solrServer = solrServerConnector.getSolrServer();
      solrServer.add(docBuffer);
      solrServer.commit();
      logger.debug("Submitted documents batch of size " + docBuffer.size() + " to Solr server.");
      docBuffer.clear();
    } catch (SolrServerException ex) {
      throw new RuntimeException("Unable to insert documents during process", ex);
    } catch (SolrException ex) {
      throw new RuntimeException("Unable to insert documents during process", ex);
    } catch (IOException iox) {
      throw new RuntimeException("Unable to insert documents during process", iox);
    }
  }

  public void setBufferSize(int bufferSize)
  {
    HOLDING_BUFFER_SIZE = bufferSize;
  }

  public SolrServerConnector getSolrServerConnector()
  {
    return solrServerConnector;
  }

  public void setSolrServerConnector(SolrServerConnector solrServerConnector)
  {
    this.solrServerConnector = solrServerConnector;
  }

}
