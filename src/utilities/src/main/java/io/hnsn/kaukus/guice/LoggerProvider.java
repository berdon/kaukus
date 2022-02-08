package io.hnsn.kaukus.guice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerProvider {
    public Logger get(String name) {
        return LoggerFactory.getLogger(name);
    }

    public Logger get() {
        return LoggerFactory.getLogger(getCallerClassName());
    }

    private static String getCallerClassName() {
        StackTraceElement ste = Thread.currentThread().getStackTrace()[3];
        return ste.getClassName();
     }
}
