package org.spoorn.tarlz4java.api;

import java.util.concurrent.ExecutorService;

/**
 * Convenience builder to create a {@link TarLz4Compressor}.
 */
public class TarLz4CompressorBuilder {

    private ExecutorService executorService = null;
    private int bufferSize = 8192;
    private int numThreads = 1;
    private boolean shouldLogProgress = false;
    private int logProgressPercentInterval = 10;
    
    public TarLz4CompressorBuilder() {
        
    }

    /**
     * Number of threads for multithreading.
     * 
     * @param numThreads Number of threads to use for multithreading
     * @return TarLz4CompressorBuilder
     */
    public TarLz4CompressorBuilder numThreads(int numThreads) {
        this.numThreads = numThreads;
        return this;
    }

    /**
     * Buffer size in bytes
     * 
     * @param bufferSize Buffer size in bytes
     * @return TarLz4CompressorBuilder
     */
    public TarLz4CompressorBuilder bufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    /**
     * Supports custom Executor Service from the caller.
     * 
     * @param executorService ExecutorService to use for multithreading
     * @return TarLz4CompressorBuilder
     */
    public TarLz4CompressorBuilder executorService(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    /**
     * Enables logging compression progress to the Logger/console.
     *
     * @param shouldLogProgress True to log progress using a Logger, else false
     * @return TarLz4CompressorBuilder
     */
    public TarLz4CompressorBuilder shouldLogProgress(boolean shouldLogProgress) {
        this.shouldLogProgress = shouldLogProgress;
        return this;
    }

    /**
     * Sets the progress percentage interval to log progress.
     *
     * @param logProgressPercentInterval The progress percentage interval to trigger progress logs
     * @return TarLz4CompressorBuilder
     */
    public TarLz4CompressorBuilder logProgressPercentInterval(int logProgressPercentInterval) {
        this.logProgressPercentInterval = logProgressPercentInterval;
        return this;
    }

    /**
     * Builds the TarLz4Compressor using parameters.
     * 
     * @return A ready TarLz4Compressor
     */
    public TarLz4Compressor build() {
        if (this.executorService == null) {
            return new TarLz4Compressor(numThreads, bufferSize, shouldLogProgress, logProgressPercentInterval);
        }
        return new TarLz4Compressor(numThreads, bufferSize, shouldLogProgress, logProgressPercentInterval, executorService);
    }
}
