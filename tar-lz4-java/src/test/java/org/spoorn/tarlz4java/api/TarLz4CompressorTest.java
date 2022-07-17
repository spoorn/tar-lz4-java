package org.spoorn.tarlz4java.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.spoorn.tarlz4java.api.TarLz4Compressor.TAR_LZ4_EXTENSION;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class TarLz4CompressorTest {
    
    private String tmpDir;
    private String randomBaseName = UUID.randomUUID().toString();
    private File test1;
    
    @BeforeEach
    public void setup() {
        tmpDir = System.getProperty("java.io.tmpdir");
        File small = new File(this.getClass().getClassLoader().getResource("small").getFile());
        test1 = Path.of(small.getPath(), "sources", "small_test1_tarlz4").toFile();
    }
    
    @Test
    public void small_overall_singleThread() throws Exception {
        TarLz4Compressor compressor = new TarLz4CompressorBuilder().build();
        Path outputPath = compressor.compress(test1.getPath(), tmpDir, randomBaseName);
        
        assertEquals(tmpDir + randomBaseName + TAR_LZ4_EXTENSION, outputPath.toString());
        assertTrue(Files.exists(outputPath));
        
        File outputFile = outputPath.toFile();
        assertTrue(outputFile.isFile());
        Files.deleteIfExists(outputPath);
    }

    @Test
    public void small_overall_multiThreaded() throws Exception {
        TarLz4Compressor compressor = new TarLz4CompressorBuilder().numThreads(6).build();
        Path outputPath = compressor.compress(test1.getPath(), tmpDir, randomBaseName);

        assertEquals(tmpDir + randomBaseName + TAR_LZ4_EXTENSION, outputPath.toString());
        assertTrue(Files.exists(outputPath));

        File outputFile = outputPath.toFile();
        assertTrue(outputFile.isFile());
        Files.deleteIfExists(outputPath);
    }

    @Test
    public void small_overall_multiThreaded_customBufferSize() throws Exception {
        TarLz4Compressor compressor = new TarLz4CompressorBuilder().numThreads(6).bufferSize(4096).build();
        Path outputPath = compressor.compress(test1.getPath(), tmpDir, randomBaseName);

        assertEquals(tmpDir + randomBaseName + TAR_LZ4_EXTENSION, outputPath.toString());
        assertTrue(Files.exists(outputPath));

        File outputFile = outputPath.toFile();
        assertTrue(outputFile.isFile());
        Files.deleteIfExists(outputPath);
    }

    @Test
    public void small_overall_multiThreaded_customExecutorService() throws Exception {
        class TestThreadFactory implements ThreadFactory {
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            
            public Thread newThread(Runnable r) {
                return new Thread(r, "tarlz4-test-" + threadNumber.getAndIncrement());
            }
        }
        
        TarLz4Compressor compressor = new TarLz4CompressorBuilder().numThreads(6).executorService(Executors.newFixedThreadPool(4, new TestThreadFactory())).build();
        Path outputPath = compressor.compress(test1.getPath(), tmpDir, randomBaseName);

        assertEquals(tmpDir + randomBaseName + TAR_LZ4_EXTENSION, outputPath.toString());
        assertTrue(Files.exists(outputPath));

        File outputFile = outputPath.toFile();
        assertTrue(outputFile.isFile());
        Files.deleteIfExists(outputPath);
    }
}
