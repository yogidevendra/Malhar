/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.contrib.r;

import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;
import junit.framework.Assert;
import org.junit.Test;

public class RMaxOperatorTest {

    /**
     * Test oper logic emits correct results
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testNodeSchemaProcessing()
    {
        RMax<Double> oper = new RMax<Double>();
        CountAndLastTupleTestSink maxSink = new CountAndLastTupleTestSink();
        oper.max.setSink(maxSink);

        oper.setup(null);
        oper.beginWindow(0); //

        Double a = new Double(2.0);
        Double b = new Double(20.0);
        Double c = new Double(1000.0);

        oper.data.process(a);
        oper.data.process(b);
        oper.data.process(c);

        a = 1.0;
        oper.data.process(a);
        a = 10.0;
        oper.data.process(a);
        b = 5.0;
        oper.data.process(b);

        b = 12.0;
        oper.data.process(b);
        c = 22.0;
        oper.data.process(c);
        c = 14.0;
        oper.data.process(c);

        a = 46.0;
        oper.data.process(a);
        b = 2.0;
        oper.data.process(b);
        a = 23.0;
        oper.data.process(a);
        oper.endWindow(); //
        oper.teardown();

        Assert.assertEquals("number emitted tuples", 1, maxSink.count);
        Assert.assertEquals("emitted high value was ", new Double(1000.0), maxSink.tuple);
    }
}