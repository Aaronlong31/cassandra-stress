package com.liba.cassandra.stress;

/**
 * Created with IntelliJ IDEA.
 * User: zhanglong
 * Date: 1/29/13
 * Time: 3:23 PM
 */
public enum Operation {
        INSERT("insert"),
        READ("read"),
        REPLAY("replay");

        private final String op;

        Operation(String val) {
            this.op = val;
        }

        public static Operation get(String op) {
            return Operation.valueOf(op.toUpperCase());
        }
}
