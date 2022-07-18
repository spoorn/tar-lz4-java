package org.spoorn.tarlz4java.logging;

import org.apache.logging.log4j.Logger;

/**
 * Wrapper over Apache logger to allow changing other log levels to INFO level depending on configured Verbosity.
 */
public class TarLz4Logger {
    
    private final Logger log;
    private final Verbosity verbosity;
    
    public TarLz4Logger(Logger log, Verbosity verbosity) {
        this.log = log;
        this.verbosity = verbosity;
    }
    
    public void info(String s, Object... args) {
        this.log.info(s, args);
    }
    
    public void debug(String s, Object... args) {
        if (this.verbosity.isAtLeast(Verbosity.DEBUG)) {
            this.log.info(s, args);
        } else {
            this.log.debug(s, args);
        }
    }
    
    public void warn(String s, Object... args) {
        if (this.verbosity.isAtLeast(Verbosity.WARN)) {
            this.log.info(s, args);
        } else {
            this.log.warn(s, args);
        }
    }
    
    public void error(String s, Object... args) {
        if (this.verbosity.isAtLeast(Verbosity.ERROR)) {
            this.log.info(s, args);
        } else {
            this.log.error(s, args);
        }
    }
}
