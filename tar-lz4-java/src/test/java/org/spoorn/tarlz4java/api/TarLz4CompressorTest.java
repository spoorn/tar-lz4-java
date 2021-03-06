package org.spoorn.tarlz4java.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.spoorn.tarlz4java.api.TarLz4Compressor.TAR_LZ4_EXTENSION;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.spoorn.tarlz4java.logging.Verbosity;
import org.spoorn.tarlz4java.util.TarLz4Util;
import org.spoorn.tarlz4java.util.concurrent.NamedThreadFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;

public class TarLz4CompressorTest {
    
    private String tmpDir;
    private final String randomBaseName = UUID.randomUUID().toString();
    private File test1;
    private File test1Excludes;
    private List<Path> resourcesCreated;
    
    @BeforeEach
    public void setup() {
        tmpDir = System.getProperty("java.io.tmpdir");
        File small = new File(this.getClass().getClassLoader().getResource("small").getFile());
        test1 = Path.of(small.getPath(), "sources", "small_test1_tarlz4").toFile();
        test1Excludes = Path.of(small.getPath(), "expected", "small_test1_tarlz4_excludes").toFile();
        this.resourcesCreated = new ArrayList<>();
    }
    
    @Test
    public void small_overall_singleThread() throws Exception {
        TarLz4Compressor compressor = new TarLz4CompressorBuilder().verbosity(Verbosity.DEBUG).shouldLogProgress(true).build();
        Path outputPath = compressor.compress(test1.getPath(), tmpDir, randomBaseName);
        resourcesCreated.add(outputPath);
        assertEquals(tmpDir + randomBaseName + TAR_LZ4_EXTENSION, outputPath.toString());
        assertTrue(Files.exists(outputPath));
        
        File outputFile = outputPath.toFile();
        assertTrue(outputFile.isFile());
        
        TarLz4Decompressor decompressor = new TarLz4DecompressorBuilder().shouldLogProgress(true).build();
        Path decompressedPath = decompressor.decompress(outputFile.getPath(), tmpDir);
        resourcesCreated.add(decompressedPath);
        assertTrue(TarLz4Util.checkDirsAreEqual(test1.toPath(), decompressedPath.resolve(test1.getName())));
    }

    @Test
    public void small_overall_singleThread_excludesFiles() throws Exception {
        TarLz4Compressor compressor = new TarLz4CompressorBuilder()
                .verbosity(Verbosity.DEBUG).shouldLogProgress(true).excludeFiles(Set.of("session.lock", "level.dat_old")).build();
        Path outputPath = compressor.compress(test1.getPath(), tmpDir, randomBaseName);
        resourcesCreated.add(outputPath);
        assertEquals(tmpDir + randomBaseName + TAR_LZ4_EXTENSION, outputPath.toString());
        assertTrue(Files.exists(outputPath));

        File outputFile = outputPath.toFile();
        assertTrue(outputFile.isFile());

        TarLz4Decompressor decompressor = new TarLz4DecompressorBuilder().shouldLogProgress(true).build();
        Path decompressedPath = decompressor.decompress(outputFile.getPath(), tmpDir);
        resourcesCreated.add(decompressedPath);
        assertTrue(TarLz4Util.checkDirsAreEqual(test1Excludes.toPath(), decompressedPath.resolve(test1.getName())));
    }

    @Test
    public void small_overall_multiThreaded() throws Exception {
        TarLz4Compressor compressor = new TarLz4CompressorBuilder().shouldLogProgress(true).numThreads(6).build();
        Path outputPath = compressor.compress(test1.getPath(), tmpDir, randomBaseName);
        resourcesCreated.add(outputPath);
        assertEquals(tmpDir + randomBaseName + TAR_LZ4_EXTENSION, outputPath.toString());
        assertTrue(Files.exists(outputPath));

        File outputFile = outputPath.toFile();
        assertTrue(outputFile.isFile());
        
        TarLz4Decompressor decompressor = new TarLz4DecompressorBuilder().shouldLogProgress(true).build();
        Path decompressedPath = decompressor.decompress(outputFile.getPath(), tmpDir);
        resourcesCreated.add(decompressedPath);
        assertTrue(TarLz4Util.checkDirsAreEqual(test1.toPath(), decompressedPath.resolve(test1.getName())));
    }

    @Test
    public void small_overall_multiThreaded_customBufferSize() throws Exception {
        TarLz4Compressor compressor = new TarLz4CompressorBuilder().shouldLogProgress(true).numThreads(6).bufferSize(4096).build();
        Path outputPath = compressor.compress(test1.getPath(), tmpDir, randomBaseName);
        resourcesCreated.add(outputPath);
        assertEquals(tmpDir + randomBaseName + TAR_LZ4_EXTENSION, outputPath.toString());
        assertTrue(Files.exists(outputPath));

        File outputFile = outputPath.toFile();
        assertTrue(outputFile.isFile());
        
        TarLz4Decompressor decompressor = new TarLz4DecompressorBuilder().shouldLogProgress(true).build();
        Path decompressedPath = decompressor.decompress(outputFile.getPath(), tmpDir);
        resourcesCreated.add(decompressedPath);
        assertTrue(TarLz4Util.checkDirsAreEqual(test1.toPath(), decompressedPath.resolve(test1.getName())));
    }

    @Test
    public void small_overall_multiThreaded_customExecutorService() throws Exception {
        TarLz4Compressor compressor = new TarLz4CompressorBuilder().shouldLogProgress(true).numThreads(6)
                .executorService(Executors.newFixedThreadPool(4, new NamedThreadFactory("TarLz4Test"))).build();
        Path outputPath = compressor.compress(test1.getPath(), tmpDir, randomBaseName);
        resourcesCreated.add(outputPath);
        assertEquals(tmpDir + randomBaseName + TAR_LZ4_EXTENSION, outputPath.toString());
        assertTrue(Files.exists(outputPath));

        File outputFile = outputPath.toFile();
        assertTrue(outputFile.isFile());

        TarLz4Decompressor decompressor = new TarLz4DecompressorBuilder().shouldLogProgress(true).build();
        Path decompressedPath = decompressor.decompress(outputFile.getPath(), tmpDir);
        resourcesCreated.add(decompressedPath);
        assertTrue(TarLz4Util.checkDirsAreEqual(test1.toPath(), decompressedPath.resolve(test1.getName())));
    }

    @Test
    public void small_overall_multiThreaded_excludeFiles() throws Exception {
        TarLz4Compressor compressor = new TarLz4CompressorBuilder().shouldLogProgress(true)
                .excludeFiles(Set.of("session.lock", "level.dat_old")).numThreads(6).build();
        Path outputPath = compressor.compress(test1.getPath(), tmpDir, randomBaseName);
        resourcesCreated.add(outputPath);
        assertEquals(tmpDir + randomBaseName + TAR_LZ4_EXTENSION, outputPath.toString());
        assertTrue(Files.exists(outputPath));

        File outputFile = outputPath.toFile();
        assertTrue(outputFile.isFile());

        TarLz4Decompressor decompressor = new TarLz4DecompressorBuilder().shouldLogProgress(true).build();
        Path decompressedPath = decompressor.decompress(outputFile.getPath(), tmpDir);
        resourcesCreated.add(decompressedPath);
        assertTrue(TarLz4Util.checkDirsAreEqual(test1Excludes.toPath(), decompressedPath.resolve(test1.getName())));
    }
    
    @AfterEach
    public void cleanup() throws IOException {
        for (Path path : resourcesCreated) {
            if (path.toFile().isFile()) {
                Files.deleteIfExists(path);
            } else {
                FileUtils.deleteDirectory(path.toFile());
            }
        }
    }
}
