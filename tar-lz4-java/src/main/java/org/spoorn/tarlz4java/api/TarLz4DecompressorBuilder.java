package org.spoorn.tarlz4java.api;

/**
 * Convenience builder to create a {@link TarLz4Decompressor}.
 */
public class TarLz4DecompressorBuilder {
    
    public TarLz4DecompressorBuilder() {
        
    }
    
    public TarLz4Decompressor build() {
        return new TarLz4Decompressor();
    }
}
