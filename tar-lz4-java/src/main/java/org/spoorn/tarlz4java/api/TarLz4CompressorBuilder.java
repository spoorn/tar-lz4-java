package org.spoorn.tarlz4java.api;

import java.util.concurrent.ExecutorService;

public class TarLz4CompressorBuilder {

    private ExecutorService executorService = null;
    private int bufferSize = 8192;
    private int numThreads = 1;
    
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
     * Builds the TarLz4Compressor using parameters.
     * 
     * @return A ready TarLz4Compressor
     */
    public TarLz4Compressor build() {
        if (this.executorService == null) {
            return new TarLz4Compressor(numThreads, bufferSize);
        }
        return new TarLz4Compressor(numThreads, bufferSize, executorService);
    }
}
