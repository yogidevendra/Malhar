package com.datatorrent.contrib.r;

import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;
import junit.framework.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class RScriptOperatorBooleanTest {
    RScript oper = new RScript("r/aBoolean.R","aBoolean", "retVal");

    @Test
    public void testString(){

        oper.setup(null);
        oper.beginWindow(0);

        CountAndLastTupleTestSink hashSink = new CountAndLastTupleTestSink();
        oper.boolOutput.setSink(hashSink);
        CountAndLastTupleTestSink hashSinkAP = new CountAndLastTupleTestSink();
        oper.boolArrayOutput.setSink(hashSinkAP);

        Map<String, RScript.REXP_TYPE> argTypeMap = new HashMap<String, RScript.REXP_TYPE>();
        argTypeMap.put("AREA", RScript.REXP_TYPE.REXP_INT);
        argTypeMap.put("DUMMY", RScript.REXP_TYPE.REXP_BOOL);
        argTypeMap.put("DUMMY_ARR", RScript.REXP_TYPE.REXP_ARRAY_BOOL);

        oper.setArgTypeMap(argTypeMap);

        HashMap map = new HashMap();

        boolean smallStates[] = new boolean[]{true, false, true, false};

        //Return the value passed for 'DUMMY'
        // A boolean value being passed and returned
        map.put("AREA", 10);
        map.put("DUMMY", false);
        map.put("DUMMY_ARR", smallStates);
        oper.inBindings.process(map);

        // Return a subset (array) of the value passed for 'DUMMY_ARR'
        // A boolean array value being passed and returned
        oper.setFunctionName("aBooleanArrayAccepted");
        map = new HashMap();
        map.put("AREA", 10000);
        map.put("DUMMY", false);
        map.put("DUMMY_ARR", smallStates);
        oper.inBindings.process(map);

        // Return a subset (array) of the value passed for 'DUMMY_ARR'
        // A boolean array value being returned
        oper.setFunctionName("aBooleanArrayReturned");
        map = new HashMap();
        map.put("AREA", 10000);
        map.put("DUMMY", false);
        map.put("DUMMY_ARR", smallStates);
        oper.inBindings.process(map);

        oper.endWindow();
        oper.teardown();

        Assert.assertEquals("Mismatch in number of boolean values returned : ", 1, hashSink.count);
        Assert.assertEquals("Mismatch in boolean values returned : ", false, hashSink.tuple);

        // Although there were two elements in the sink, only last one is accessible. 
        Assert.assertEquals("Mismatch in number of elements returned : ", 2, hashSinkAP.count);
        Boolean [] bArr = (Boolean []) hashSinkAP.tuple;
        // Check few random values. Expected results are obtained by actually running the function in R
        Assert.assertEquals("Boolean array return mismatch : ", (Boolean)false, bArr[5]);
        Assert.assertEquals("Boolean array return mismatch : ", (Boolean)true, bArr[6]);
        Assert.assertEquals("Boolean array return mismatch : ", (Boolean)true, bArr[7]);
        Assert.assertEquals("Boolean array return mismatch : ", (Boolean)false, bArr[8]);
        
        // There are 50 states in US and hence should return 50 values.
        Assert.assertEquals("Boolean array length mismatch : ", 50, bArr.length);
    }


}
