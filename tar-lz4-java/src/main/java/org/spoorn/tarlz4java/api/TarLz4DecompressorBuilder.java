package org.spoorn.tarlz4java.api;

import org.spoorn.tarlz4java.logging.Verbosity;

/**
 * Convenience builder to create a {@link TarLz4Decompressor}.
 */
public class TarLz4DecompressorBuilder {
    
    private boolean shouldLogProgress = false;
    private int logProgressPercentInterval = 10;
    private Verbosity verbosity = Verbosity.WARN;
    
    public TarLz4DecompressorBuilder() {
        
    }

    /**
     * Enables logging compression progress to the Logger/console.
     * 
     * Note: this is an approximation, as the final size of the decompressed file is not accurately determined beforehand.
     *
     * @param shouldLogProgress True to log progress using a Logger, else false
     * @return TarLz4DecompressorBuilder
     */
    public TarLz4DecompressorBuilder shouldLogProgress(boolean shouldLogProgress) {
        this.shouldLogProgress = shouldLogProgress;
        return this;
    }

    /**
     * Sets the progress percentage interval to log progress.
     *
     * @param logProgressPercentInterval The progress percentage interval to trigger progress logs
     * @return TarLz4DecompressorBuilder
     */
    public TarLz4DecompressorBuilder logProgressPercentInterval(int logProgressPercentInterval) {
        this.logProgressPercentInterval = logProgressPercentInterval;
        return this;
    }

    /**
     * Sets the verbosity level.  See {@link Verbosity} for documentation.
     *
     * @param verbosity Verbosity level
     * @return TarLz4DecompressorBuilder
     */
    public TarLz4DecompressorBuilder verbosity(Verbosity verbosity) {
        this.verbosity = verbosity;
        return this;
    }
    
    public TarLz4Decompressor build() {
        return new TarLz4Decompressor(shouldLogProgress, logProgressPercentInterval, verbosity);
    }
}
