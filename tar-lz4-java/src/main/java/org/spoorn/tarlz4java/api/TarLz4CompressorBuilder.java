package org.spoorn.tarlz4java.api;

import java.util.concurrent.ExecutorService;

public class TarLz4CompressorBuilder {

    private ExecutorService executorService = null;
    private int bufferSize = 8192;
    private int numThreads = 1;
    
    public TarLz4CompressorBuilder() {
        
    }
    
    public TarLz4CompressorBuilder numThreads(int numThreads) {
        this.numThreads = numThreads;
        return this;
    }
    
    public TarLz4CompressorBuilder bufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }
    
    public TarLz4CompressorBuilder executorService(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }
    
    public TarLz4Compressor build() {
        if (this.executorService == null) {
            return new TarLz4Compressor(numThreads, bufferSize);
        }
        return new TarLz4Compressor(numThreads, bufferSize, executorService);
    }
}
