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

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

public class RScriptOperatorStrTest {

    RScript oper = new RScript("r/aString.R","aString", "retVal");

    @Test
    public void testString(){

        oper.setup(null);
        oper.beginWindow(0);

        CountAndLastTupleTestSink hashSink = new CountAndLastTupleTestSink();
        oper.strOutput.setSink(hashSink);

        Map<String, RScript.REXP_TYPE> argTypeMap = new HashMap<String, RScript.REXP_TYPE>();
        argTypeMap.put("str1", RScript.REXP_TYPE.REXP_STR);
        argTypeMap.put("str2", RScript.REXP_TYPE.REXP_STR);
        argTypeMap.put("seperator", RScript.REXP_TYPE.REXP_STR);
        oper.setArgTypeMap(argTypeMap);

        HashMap map = new HashMap();

        map.put("str1", "Hello ");
        map.put("str2", " World");
        map.put("seperator", " ");

        oper.inBindings.process(map);
        Assert.assertEquals("Mismatch in number of string emitted : ",  1, hashSink.count);
        Assert.assertEquals("Mismatch in returned string : ",  "Hello   World", hashSink.tuple);
        map = new HashMap();

        map.put("str1", "Have a");
        map.put("str2", "great day!!!");
        map.put("seperator", "-");
        oper.inBindings.process(map);

        oper.endWindow();
        oper.teardown();

        Assert.assertEquals("Mismatch in number of string emitted : ",  2, hashSink.count);
        Assert.assertEquals("Mismatch in returned string : ",  "Have a-great day!!!", hashSink.tuple);
    }
}
