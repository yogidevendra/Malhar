/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.testbench;

import java.util.Arrays;
import java.util.Random;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;


/**
 * Generates Random samples from array of items based on given weight bias.
 * This class is mainly meant to be used by demo apps.  
 * 
 * <b>Properties</b>:
 * <b>tupleBlast</b> is the total amount of tuples sent by the node before handing over control. 
 * <b>sleepMs</b> sleep time in miliseconds after emiting tuples.
 * <b>items</b> array of items to choose from.
 * <b>weights</b> itemwise weights bias for the item selection.
 */

public class WeightBiasRandomInputOperator<T> extends BaseOperator implements InputOperator{

  public final transient DefaultOutputPort<T> outPort = new DefaultOutputPort<T>();
  private transient int count;
  private int sleepMs = 10;
  private long tupleBlast = 10;
  
  private transient WeightedBiasRandomElementGenerator<T> weightedBiasRandomElementGenerator;
  private T[] items;
  private int[] weights;

  /* (non-Javadoc)
   * @see com.datatorrent.api.BaseOperator#setup(com.datatorrent.api.Context.OperatorContext)
   */
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    weightedBiasRandomElementGenerator = new WeightedBiasRandomElementGenerator<T>(items, weights);
    weightedBiasRandomElementGenerator.initialize();
    
  }
  
  /* (non-Javadoc)
   * @see com.datatorrent.api.InputOperator#emitTuples()
   */
  @Override
  public void emitTuples()
  {
    for(int i = 0 ; i < tupleBlast; i++) {
      T element = weightedBiasRandomElementGenerator.getRandomElement();
      outPort.emit(element);
    }
    try {
      Thread.sleep(sleepMs);
    } catch(Exception ex) {
      System.out.println(ex.getMessage());
    }
    count++;
    
  }
  
  /* (non-Javadoc)
   * @see com.datatorrent.api.BaseOperator#endWindow()
   */
  @Override
  public void endWindow()
  {
    //System.out.println("emitTuples called  " + count + " times in this window");
    count = 0;
  }
  
  /**
   * @return the sleepMs
   */
  public int getSleepMs()
  {
    return sleepMs;
  }

  /**
   * @param sleepMs the sleepMs to set
   */
  public void setSleepMs(int sleepMs)
  {
    this.sleepMs = sleepMs;
  }

  /**
   * @return the tupleBlast
   */
  public long getTupleBlast()
  {
    return tupleBlast;
  }

  /**
   * @param tupleBlast the tupleBlast to set
   */
  public void setTupleBlast(long tupleBlast)
  {
    this.tupleBlast = tupleBlast;
  }

  /**
   * @return the items
   */
  public T[] getItems()
  {
    return items;
  }

  /**
   * @param items the items to set
   */
  public void setItems(T[] items)
  {
    this.items = items;
  }

  /**
   * @return the weights
   */
  public int[] getWeights()
  {
    return weights;
  }

  /**
   * @param weights the weights to set
   */
  public void setWeights(int[] weights)
  {
    this.weights = weights;
  }

 
  
  private class WeightedBiasRandomElementGenerator<T>  
  {
    private T[] items;
    private int[] weights;
    private int[] cumulativeWeights;
    private Random random;

    /**
     * @param items
     * @param weights
     */
    public WeightedBiasRandomElementGenerator(T[] items, int[] weights)
    {
      super();
      this.items = items;
      this.weights = weights;
    }

    /**
     * initialize cumulativeWeights
     */
    public void initialize()
    {
      random = new Random();
      
      int sumTillNow = 0;
      cumulativeWeights = new int[weights.length];
      for (int i = 0; i < weights.length; i++) {
        sumTillNow += weights[i];
        cumulativeWeights[i] = sumTillNow;
      }
    }

    /**
     * Generates random index honoring the given weights 
     * and returns corresponding item from the list.
     * @return
     */
    T getRandomElement()
    {
      // Generate random sample between 1 to sum(weights).
      // sum(weights) is available in cumulativeWeights[cumulativeWeights.length-1]
      int rand = random.nextInt(cumulativeWeights[cumulativeWeights.length-1]);
      int sample = rand + 1;
      //Find out index for which cumulativeWeight is less than or equal to sample
      int index = Arrays.binarySearch(cumulativeWeights, sample);
      //If exact match not found then binarysearch returns -(insertion point) -1
      //convert it to insertion point
      if (index < 0) {
        index = -(index+1);
      }
      return items[index];
    }

  
}
  
  /**
   * Demo run
   */
  public void demoRun()
  {
    
  }

  public static void main(String[] args)
  {
    new WeightBiasRandomInputOperator<String>().demoRun(); 
  }
}
