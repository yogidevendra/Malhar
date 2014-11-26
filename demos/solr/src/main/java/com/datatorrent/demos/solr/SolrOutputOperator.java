package com.datatorrent.demos.solr;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.solr.common.SolrInputDocument;

import com.datatorrent.contrib.solr.AbstractSolrOutputOperator;
import com.datatorrent.contrib.solr.HttpSolrServerConnector;

/**
 * Properties:<br>
 * <b>tuplesBlast</b>: Number of tuples emitted in each burst<br>
 * <b>bufferSize</b>: Size of holding buffer<br>
 * <br>
 */
public class SolrOutputOperator extends AbstractSolrOutputOperator<ProductInfo>
{
  @Override
  public void initializeSolrServerConnector()
  {
    solrServerConnector = new HttpSolrServerConnector();
  }

  @Override
  public SolrInputDocument convertTuple(ProductInfo prodInfo)
  {
    Calendar calendar = new GregorianCalendar();
    Date date = calendar.getTime();
    String solrFormateDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(date);
    SolrInputDocument doc = new SolrInputDocument();
    long docId = System.currentTimeMillis();
    doc.addField("id", docId);
    doc.addField("name", String.valueOf(docId));
    doc.addField("categoryId_i", prodInfo.getCategoryId());
    doc.addField("productId_i", prodInfo.getProductId());
    doc.addField("queryType_t", prodInfo.getQueryType());
    doc.addField("createDate_dt", solrFormateDate);
    return doc;
  }

}
