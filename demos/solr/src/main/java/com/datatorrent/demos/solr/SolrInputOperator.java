package com.datatorrent.demos.solr;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.servlet.SolrRequestParsers;

import com.datatorrent.contrib.solr.AbstractSolrInputOperator;
import com.datatorrent.contrib.solr.HttpSolrServerConnector;

public class SolrInputOperator extends AbstractSolrInputOperator<ProductInfo>
{
  private static final Calendar calendar = new GregorianCalendar();
  private static Date lastQueryTime = new Date(-1L);

  @Override
  public void initializeSolrServerConnector()
  {
    solrServerConnector = new HttpSolrServerConnector();
  }

  @Override
  protected void emitTuple(SolrDocument document)
  {
    int categorId = ((Integer) document.getFieldValue("categoryId_i")).intValue();
    int productId = ((Integer) document.getFieldValue("productId_i")).intValue();
    String queryType = document.getFieldValue("queryType_t").toString();
    ProductInfo productInfo = new ProductInfo(categorId, productId, queryType);
    outputPort.emit(productInfo);
  }

  @Override
  public SolrParams getQueryParams()
  {
    String time = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(lastQueryTime);
    String query = String.format("q=createDate_dt: [%s TO *]&rows=2147483647", time);
    SolrParams solrParams = SolrRequestParsers.parseQueryString(query);
    lastQueryTime = calendar.getTime();
    return solrParams;
  }

}
