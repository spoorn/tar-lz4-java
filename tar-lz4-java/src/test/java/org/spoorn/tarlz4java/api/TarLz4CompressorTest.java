package org.spoorn.tarlz4java.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.spoorn.tarlz4java.api.TarLz4Compressor.TAR_LZ4_EXTENSION;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class TarLz4CompressorTest {
    
    private String tmpDir;
    private File test1;
    private File test1Expected;
    
    @BeforeEach
    public void setup() {
        tmpDir = System.getProperty("java.io.tmpdir");
        File small = new File(this.getClass().getClassLoader().getResource("small").getFile());
        test1 = Path.of(small.getPath(), "sources", "small_test1_tarlz4").toFile();
        test1Expected = Path.of(small.getPath(), "expected", "small_test1_tarlz4.tar.lz4").toFile();
    }
    
    @Test
    public void small_overall_singleThread() {
        TarLz4Compressor compressor = new TarLz4CompressorBuilder().build();
        Path outputPath = compressor.compress(test1.getPath(), tmpDir);
        
        assertEquals(tmpDir + test1.getName() + TAR_LZ4_EXTENSION, outputPath.toString());
        assertTrue(Files.exists(outputPath));
        
        File outputFile = outputPath.toFile();
        assertTrue(outputFile.isFile());
        long t1 = test1Expected.length();
        long t2 = outputFile.length();
    }

    @Test
    public void small_overall_multiThreaded() {
        TarLz4Compressor compressor = new TarLz4CompressorBuilder().numThreads(6).build();
        Path outputPath = compressor.compress(test1.getPath(), tmpDir);

        assertEquals(tmpDir + test1.getName() + TAR_LZ4_EXTENSION, outputPath.toString());
        assertTrue(Files.exists(outputPath));

        File outputFile = outputPath.toFile();
        assertTrue(outputFile.isFile());
    }

    @Test
    public void small_overall_multiThreaded_customBufferSize() {
        TarLz4Compressor compressor = new TarLz4CompressorBuilder().numThreads(6).bufferSize(4096).build();
        Path outputPath = compressor.compress(test1.getPath(), tmpDir);

        assertEquals(tmpDir + test1.getName() + TAR_LZ4_EXTENSION, outputPath.toString());
        assertTrue(Files.exists(outputPath));

        File outputFile = outputPath.toFile();
        assertTrue(outputFile.isFile());
    }

    @Test
    public void small_overall_multiThreaded_customExecutorService() {
        class TestThreadFactory implements ThreadFactory {
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            
            public Thread newThread(Runnable r) {
                return new Thread(r, "tarlz4-test-" + threadNumber.getAndIncrement());
            }
        }
        
        TarLz4Compressor compressor = new TarLz4CompressorBuilder().numThreads(6).executorService(Executors.newFixedThreadPool(4, new TestThreadFactory())).build();
        Path outputPath = compressor.compress(test1.getPath(), tmpDir);

        assertEquals(tmpDir + test1.getName() + TAR_LZ4_EXTENSION, outputPath.toString());
        assertTrue(Files.exists(outputPath));

        File outputFile = outputPath.toFile();
        assertTrue(outputFile.isFile());
    }
}
