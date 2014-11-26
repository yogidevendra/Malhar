package com.datatorrent.demos.solr;

public class ProductInfo
{
  private int categoryId;
  private int productId;
  private String queryType;

  public ProductInfo(int categoryId, int productId, String queryType)
  {
    this.categoryId = categoryId;
    this.productId = productId;
    this.queryType = queryType;
  }

  public int getCategoryId()
  {
    return categoryId;
  }

  public int getProductId()
  {
    return productId;
  }

  public String getQueryType()
  {
    return queryType;
  }

  @Override
  public String toString()
  {
    return "CategoryId: " + categoryId + " ProductId: " + productId + " Query: " + queryType;
  }

}
