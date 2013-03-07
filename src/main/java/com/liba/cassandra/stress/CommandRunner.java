package com.liba.cassandra.stress;

import me.prettyprint.cassandra.service.CassandraHost;
import org.apache.cassandra.utils.LatencyTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IntelliJ IDEA.
 * User: zhanglong
 * Date: 1/29/13
 * Time: 3:23 PM
 */
public class CommandRunner {

    private static final Logger log = LoggerFactory.getLogger(CommandRunner.class);

    final Map<CassandraHost, LatencyTracker> latencies;
    CountDownLatch doneSignal;
    private Operation previousOperation;
    private CommandArgs previousCommandArgs;

    public CommandRunner(Set<CassandraHost> cassandraHosts) {
        latencies = new ConcurrentHashMap<CassandraHost, LatencyTracker>();
        for (CassandraHost host : cassandraHosts) {
            latencies.put(host, new LatencyTracker());
        }
    }


    public void processCommand(CommandArgs commandArgs) throws Exception {
        if ( commandArgs.getOperation() != Operation.REPLAY ) {
            previousOperation = commandArgs.getOperation();
            previousCommandArgs = commandArgs;
        }

        ExecutorService exec = Executors.newFixedThreadPool(commandArgs.threads);
        log.info(previousOperation + commandArgs.toString());
        long currentTime = System.currentTimeMillis();
        for (int execCount = 0; execCount < commandArgs.getExecutionCount(); execCount++) {
            doneSignal = new CountDownLatch(commandArgs.threads);
            for (int i = 0; i < commandArgs.threads; i++) {
                log.debug("submitting task {}", i+1);
                exec.submit(getCommandInstance(i*commandArgs.getKeysPerThread(), commandArgs, this));
            }
            log.debug("all tasks submitted for execution for execution {} of {}", execCount+1, commandArgs.getExecutionCount());
            doneSignal.await();
        }
        log.info("Cost {}ms", System.currentTimeMillis()-currentTime);


        exec.shutdown();

        for (CassandraHost host : latencies.keySet()) {
            LatencyTracker latency = latencies.get(host);
            log.debug("Latency for host {}:\n Op Count {} \nRecentLatencyHistogram {} \nRecent Latency Micros {} \nTotalLatencyHistogram {} \nTotalLatencyMicros {}",
                    new Object[]{host.getName(), latency.getOpCount(), latency.getRecentLatencyHistogramMicros(), latency.getRecentLatencyMicros(),
                            latency.getTotalLatencyHistogramMicros(), latency.getTotalLatencyMicros()});
        }
    }

    private StressCommand getCommandInstance(int startKeyArg, CommandArgs commandArgs, CommandRunner commandRunner) {

        int startKey = commandArgs.startKey + startKeyArg;
        if ( log.isDebugEnabled() ) {
            log.debug("Command requested with starting key pos {} and op {}", startKey, commandArgs.getOperation());
        }

        Operation operation = commandArgs.getOperation();
        if ( operation.equals(Operation.REPLAY )) {
            operation = previousOperation;
            commandArgs = previousCommandArgs;
        }
        switch(operation) {
            case INSERT:
                return new InsertCommand(startKey, commandArgs, commandRunner);
            case READ:
                return new ReadCommand(startKey, commandArgs, commandRunner);
        };
        return new InsertCommand(startKey, commandArgs, commandRunner);
    }

}
