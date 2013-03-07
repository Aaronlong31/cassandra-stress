package com.liba.cassandra.stress;

import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.cassandra.utils.LatencyTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * Created with IntelliJ IDEA.
 * User: zhanglong
 * Date: 1/29/13
 * Time: 3:12 PM
 */
public abstract class StressCommand implements Callable<Void> {

    private static Logger log = LoggerFactory.getLogger(StressCommand.class);

    protected final CommandArgs commandArgs;
    protected final int startKey;
    protected final CommandRunner commandRunner;

    public StressCommand(int startKey, CommandArgs commandArgs, CommandRunner commandRunner) {
        this.commandArgs = commandArgs;
        this.startKey = startKey;
        this.commandRunner = commandRunner;
    }

    protected void executeMutator(Mutator mutator, int rows) {
        try {
            MutationResult mr = mutator.execute();
            // could be null here when our batch size is zero
            if ( mr.getHostUsed() != null ) {
                LatencyTracker writeCount = commandRunner.latencies.get(mr.getHostUsed());
                if ( writeCount != null )
                    writeCount.addMicro(mr.getExecutionTimeMicro());
            }
            mutator.discardPendingMutations();

            log.info("executed batch of {}. {} of {} complete", new Object[]{commandArgs.batchSize, rows, commandArgs.getKeysPerThread()});

        } catch (Exception ex){
            log.error("Problem executing insert:",ex);
        }
    }

}
