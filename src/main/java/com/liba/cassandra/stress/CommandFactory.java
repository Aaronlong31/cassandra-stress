package com.liba.cassandra.stress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: zhanglong
 * Date: 1/29/13
 * Time: 3:25 PM
 */
public class CommandFactory {
    private static Logger log = LoggerFactory.getLogger(CommandFactory.class);

    public static StressCommand getInstance(int startKey, CommandArgs commandArgs, CommandRunner commandRunner) {
        switch(commandArgs.getOperation()) {
            case INSERT:
                return new InsertCommand(startKey, commandArgs, commandRunner);
            case READ:
                return new ReadCommand(startKey, commandArgs, commandRunner);
        };
        log.info("Runnig default Insert command...");
        return new InsertCommand(startKey, commandArgs, commandRunner);
    }
}
