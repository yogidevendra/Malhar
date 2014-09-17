package com.datatorrent.demos.oldfaithful;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.contrib.r.RScript;

public class FaithfulRScript extends RScript {

    private List<FaithfulKey> readingsList = new ArrayList<FaithfulKey>();
    private int elapsedTime;

    public FaithfulRScript()
    {
        super();
    }

    public FaithfulRScript(String rScriptFilePath, String rFunction, String returnVariable)
    {
        super(rScriptFilePath, rFunction, returnVariable);
    }

    @InputPortFieldAnnotation(name = "faithfulInput", optional = true)
    public final transient DefaultInputPort<FaithfulKey> faithfulInput = new DefaultInputPort<FaithfulKey>()
    {
        @Override
        public void process(FaithfulKey tuple)
        {
            // Create a map of ("String", values) to be passed to the process
            // function in the RScipt operator's process()
           readingsList.add(tuple);

        }

    };

    @InputPortFieldAnnotation(name = "inputElapsedTime", optional = true)
    public final transient DefaultInputPort<Integer> inputElapsedTime= new DefaultInputPort<Integer>()
    {
        @Override
        public void process(Integer eT)
        {
            elapsedTime = eT;
        }
    };

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
    }

        @Override
    public void endWindow() {

        if (readingsList.size() == 0) return;

        double[] eruptionDuration = new double[readingsList.size()];
        int[] waitingTime = new int[readingsList.size()];

        for (int i = 0; i < readingsList.size(); i++){
            eruptionDuration[i] = readingsList.get(i).getEruptionDuration();
            waitingTime[i] = readingsList.get(i).getWaitingTime();
        }
        HashMap map = new HashMap();

        map.put("ELAPSEDTIME", elapsedTime);
        map.put("ERUPTIONS", eruptionDuration);
        map.put("WAITING", waitingTime);

        super.process(map);

    };
}