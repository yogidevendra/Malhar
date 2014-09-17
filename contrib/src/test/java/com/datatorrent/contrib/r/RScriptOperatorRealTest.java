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

import com.datatorrent.lib.testbench.*;
import junit.framework.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class RScriptOperatorRealTest {
    RScript oper = new RScript("r/aReal.R", "aReal", "retVal");

    @Test
    public void testReal(){

//        oper.setScriptFilePath("r/aReal.R");
//        oper.setFunctionName("aReal");
//        oper.setReturnVariable("retVal");
//        oper.setRuntimeFileCopy(true);

        oper.setup(null);
        oper.beginWindow(0);

        CountAndLastTupleTestSink hashSink = new CountAndLastTupleTestSink();
        oper.doubleOutput.setSink(hashSink);

        Map<String, RScript.REXP_TYPE> argTypeMap = new HashMap<String, RScript.REXP_TYPE>();
        argTypeMap.put("num1", RScript.REXP_TYPE.REXP_DOUBLE);
        argTypeMap.put("num2", RScript.REXP_TYPE.REXP_DOUBLE);

        oper.setArgTypeMap(argTypeMap);

        HashMap map = new HashMap();

        map.put("num1", 5.2);
        map.put("num2", 12.4);

        oper.inBindings.process(map);
        Assert.assertEquals("Mismatch in number of elements emitted : ", 1, hashSink.count);
        Assert.assertEquals("Mismatch in number of elements emitted : ", (Double)17.6, (Double)hashSink.tuple);
        
        map = new HashMap();

        map.put("num1", 10.1);
        map.put("num2", 12.6);
        oper.inBindings.process(map);

        oper.endWindow();
        oper.teardown();

        Assert.assertEquals("Mismatch in number of elements emitted : ", 2, hashSink.count);
        Assert.assertEquals("Mismatch in number of elements emitted : ", (Double)22.7, (Double)hashSink.tuple);
    }
}
