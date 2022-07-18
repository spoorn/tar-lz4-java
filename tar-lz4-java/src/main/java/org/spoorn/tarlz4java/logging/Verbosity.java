package org.spoorn.tarlz4java.logging;

/**
 * Used when configuring the builder for TarLz4 Compressor/Decompressor to transform the log level to INFO level to
 * allow for more verbose logging.
 * 
 * The levels of Verbosity go in order from least to most restrictive:
 *      DEBUG < INFO < WARN < ERROR < NONE
 *      
 * Setting the Verbosity when creating a {@link org.spoorn.tarlz4java.api.TarLz4Compressor} or {@link org.spoorn.tarlz4java.api.TarLz4Decompressor}
 * will change log lines to INFO level, if they are "at least" the Verbosity level.
 * 
 * Meaning if Verbosity is set to DEBUG, debug/warn/error statements will turn into INFO level.  If Verbosity is set to WARN,
 * only warn/error statements will turn into INFO level.  Verbosity ERROR will only set error statements to INFO, and
 * NONE means no logging.
 * 
 * Note: this only overrides a log level to INFO level, so if your log configuration is set to something more restrictive
 * than INFO, such as WARN or ERROR, it will not log.
 */
public enum Verbosity {

    DEBUG,
    INFO,
    WARN,
    ERROR,
    NONE;
    
    public boolean isAtLeast(Verbosity verbosity) {
        Verbosity[] verbs = Verbosity.values();
        for (int i = 0; i < verbs.length; i++) {
            if (verbs[i] == this) {
                return true;
            } else if (verbs[i] == verbosity) {
                return false;
            }
        }
        return false;
    }
}
