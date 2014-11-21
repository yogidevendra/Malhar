/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.testbench;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.Sink;
import com.datatorrent.lib.testbench.WeightBiasRandomInputOperator;

/**
 * Unit test for WeightedBiasRandomElementGenerator
 */
public class WeightedBiasRandomElementGeneratorTest
{

  @Test
  public void testWeightedGeneration()
  {
    String[] fruits = { "Apple", "Banana", "Chikoo" };
    int[] weights = { 3, 1, 2 };
    WeightBiasRandomInputOperator operator = new WeightBiasRandomInputOperator<String>();
    operator.setTupleBlast(60000);
    operator.setItems(fruits);
    operator.setWeights(weights);

    final Map<String, MutableInt> counts = new HashMap<String, MutableInt>();

    Sink sink = new Sink<String>() {
      /*
       * (non-Javadoc)
       * 
       * @see com.datatorrent.api.Sink#put(java.lang.Object)
       */
      @Override
      public void put(String tuple)
      {
        MutableInt count = counts.get(tuple);
        if (count == null) {
          count = new MutableInt(0);
          counts.put(tuple, count);
        }
        count.increment();
      }

      /*
       * (non-Javadoc)
       * 
       * @see com.datatorrent.api.Sink#getCount(boolean)
       */
      @Override
      public int getCount(boolean reset)
      {
        int total = 0;
        for (String key : counts.keySet()) {
          total += counts.get(key).intValue();
        }
        return total;
      }
    };

    operator.outPort.setSink(sink);
    operator.setup(null);
    for (int windowId = 1; windowId <= 1; windowId++) {
      operator.beginWindow(windowId);
      operator.emitTuples();
      operator.endWindow();
    }

    operator.teardown();

    float wtSum = 0.0f;
    for (int wt : weights) {
      wtSum += wt;
    }

    for (int i = 0; i < fruits.length; i++) {
      float wtRatio = weights[i] / wtSum;
      float ratio = counts.get(fruits[i]).intValue() / (60000.0f * wtRatio);
      
      Assert.assertTrue("Ratio not within bounds for given weights",ratio >= 0.9 && ratio <= 1.1);
    }
  }
}
