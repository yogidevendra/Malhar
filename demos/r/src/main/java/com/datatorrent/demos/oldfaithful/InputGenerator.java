package com.datatorrent.demos.oldfaithful;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

// The InputGenerator operator is used to generate input for the 'Old Faithful Geyser" application.
// This application accepts readings for the waiting time and the subsequent eruption duration
// of the 'Old Faithful' and based on this data, tries to predict the eruption duration of the next
// eruption given the elapsed time since the last eruption.
// The training data is generated for an application window and consists of multiple
// waiting times and eruption duration values.
// For every application window, it generates only one 'elapsed time' input for which the
// prediction would be made.

public class InputGenerator implements InputOperator {

	@SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(InputGenerator.class);
    private int blastCount = 10000;
    private Random random = new Random();

    @OutputPortFieldAnnotation(name = "outputPort")
    public final transient DefaultOutputPort<FaithfulKey> outputPort = new DefaultOutputPort<FaithfulKey>();

    @OutputPortFieldAnnotation(name = "elapsedTime")
    public final transient DefaultOutputPort<Integer> elapsedTime = new DefaultOutputPort<Integer>();


    public void setBlastCount(int blastCount)
    {
        this.blastCount = blastCount;
    }

    @Override
    public void beginWindow(long windowId)
    {
    }

    @Override
    public void endWindow()
    {
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
    }

    @Override
    public void teardown()
    {
    }

    private int nextRandomId(int min, int max)
    {
        int id;
        do {
            id = (int)Math.abs(Math.round(random.nextGaussian() * max));
        }
        while (id >= max);

        if (id < min){
            id = min;
        }
        return id;
    }

    @Override
    public void emitTuples()
    {
        boolean elapsedTimeSent = false;

        try {
            for (int i = 0; i <blastCount; ++i) {
                int waitingTime = nextRandomId(3600, 36000);

                double eruptionDuration = -2.15 + 0.05 * waitingTime;
                emitTuple(eruptionDuration, waitingTime);

                if (!elapsedTimeSent){
                    int eT = 0;

                    if (i % 100 == 0){
                        eT = 54 + waitingTime;

                        emitElapsedTime(eT);
                        elapsedTimeSent = true;
                    }
                }
            }
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void emitTuple(double eruptionDuration, int waitingTime) {
        FaithfulKey faithfulkey = new FaithfulKey();

        faithfulkey.setEruptionDuration(eruptionDuration);
        faithfulkey.setWaitingTime(waitingTime);

        this.outputPort.emit(faithfulkey);
    }

    private void emitElapsedTime(int eT) {
        this.elapsedTime.emit(eT);
    }
}
