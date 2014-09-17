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
import com.datatorrent.lib.testbench.HashTestSink;
import com.datatorrent.lib.testbench.SumTestSink;
import junit.framework.Assert;
import org.junit.Test;
import org.rosuda.JRI.REXP;

import java.util.HashMap;
import java.util.Map;

public class RScriptOperatorIntTest {

    RScript oper = new RScript("r/anInteger.R", "anInteger","retVal");

    @Test
    public void testInt(){

        oper.setup(null);
        oper.beginWindow(0);

        CountAndLastTupleTestSink hashSink = new CountAndLastTupleTestSink();
        oper.intOutput.setSink(hashSink);

        Map<String, RScript.REXP_TYPE> argTypeMap = new HashMap<String, RScript.REXP_TYPE>();
        argTypeMap.put("num1", RScript.REXP_TYPE.REXP_INT);
        argTypeMap.put("num2", RScript.REXP_TYPE.REXP_INT);
        oper.setArgTypeMap(argTypeMap);

        HashMap map = new HashMap();
        map.put("num1", 5);
        map.put("num2", 12);
        oper.inBindings.process(map);
        Assert.assertEquals("Mismatch in number of elements emitted : ",  1, hashSink.getCount());
        Assert.assertEquals("Expected result mismatch: ",  17, Integer.parseInt(hashSink.tuple.toString()));

        map = new HashMap();
        map.put("num1", 102);
        map.put("num2", 12);
        oper.inBindings.process(map);
        Assert.assertEquals("Mismatch in number of elements emitted : ",  2, hashSink.getCount());
        Assert.assertEquals("Expected result mismatch: ",  114, Integer.parseInt(hashSink.tuple.toString()));

        oper.endWindow();
        oper.teardown();

    }
}