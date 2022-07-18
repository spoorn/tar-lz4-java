package org.spoorn.tarlz4java.api;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.spoorn.tarlz4java.core.TarLz4CompressTask;
import org.spoorn.tarlz4java.util.TarLz4Util;
import org.spoorn.tarlz4java.util.concurrent.NamedThreadFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Log4j2
public class TarLz4Compressor {
    
    public static final String TAR_LZ4_EXTENSION = ".tar.lz4";
    private static final String TMP_SUFFIX = ".tmp";
    private static final String THREAD_NAME = "TarLz4CompressTask";

    private final ExecutorService executorService;
    private final int bufferSize;
    private final int numThreads;
    private final boolean shouldLogProgress;
    private final int logProgressPercentInterval;
    
    public TarLz4Compressor(int numThreads, int bufferSize, boolean shouldLogProgress, int logProgressPercentInterval) {
        // We'll submit our runnable tasks using an executor service with `numThreads` threads in the pool
       this(numThreads, bufferSize, shouldLogProgress, logProgressPercentInterval, Executors.newFixedThreadPool(numThreads, new NamedThreadFactory(THREAD_NAME)));
    }

    public TarLz4Compressor(int numThreads, int bufferSize, boolean shouldLogProgress, int logProgressPercentInterval, ExecutorService executorService) {
        this.numThreads = numThreads;
        this.bufferSize = bufferSize;
        this.executorService = executorService;
        this.shouldLogProgress = shouldLogProgress;
        this.logProgressPercentInterval = logProgressPercentInterval;
    }

    /**
     * Allows setting the global log level for TarLz4Compressor instances.
     * This is useful for enabling debug logs by calling <code>TarLz4Compressor.setGlobalLogLevel(Level.DEBUG);</code>
     * 
     * @param level Log level to set to
     */
    public static void setGlobalLogLevel(Level level) {
        Configurator.setLevel(log.getName(), level);
        TarLz4CompressTask.setGlobalLogLevel(level);
    }

    /**
     * Compresses a source path into a Tar Archive using LZ4 compression.  Outputs a .tar.lz4 file to the destination path.
     * The .tar.lz4 file name will be the same as the source directory.
     *
     * @param sourcePath Source path.  Should be the path to the directory to compress.
     * @param destinationPath Destination path.  Should be the path to a directory where the .tar.lz4 will go.
     * @return Path to the output file, else a RuntimeException would have been thrown
     */
    public Path compress(Path sourcePath, Path destinationPath) {
        return compress(sourcePath.toString(), destinationPath.toString());
    }

    /**
     * Compresses a source path into a Tar Archive using LZ4 compression.  Outputs a .tar.lz4 file to the destination path.
     * The .tar.lz4 file name will be the same as the source directory.
     *
     * @param sourcePath Source path.  Should be the path to the directory to compress.
     * @param destinationPath Destination path.  Should be the path to a directory where the .tar.lz4 will go.
     * @param outputFileBaseName Output file base name, excluding the extension.  This wraps the source
     *                           under a new directory with this base name in the archive.
     * @return Path to the output file, else a RuntimeException would have been thrown
     */
    public Path compress(Path sourcePath, Path destinationPath, String outputFileBaseName) {
        return compress(sourcePath.toString(), destinationPath.toString(), outputFileBaseName);
    }

    /**
     * Compresses a source path into a Tar Archive using LZ4 compression.  Outputs a .tar.lz4 file to the destination path.
     * The .tar.lz4 file name will be the same as the source directory.
     *
     * @param sourcePath Source path.  Should be the path to the directory to compress.
     * @param destinationPath Destination path.  Should be the path to a directory where the .tar.lz4 will go.
     * @return Path to the output file, else a RuntimeException would have been thrown
     */
    public Path compress(String sourcePath, String destinationPath) {
        return compress(sourcePath, destinationPath, new File(sourcePath).getName());
    }

    /**
     * Compresses a source path into a Tar Archive using LZ4 compression.  Outputs a .tar.lz4 file to the destination path.
     * The .tar.lz4 file name will be the same as the source directory.
     * 
     * @param sourcePath Source path.  Should be the path to the directory to compress.
     * @param destinationPath Destination path.  Should be the path to a directory where the .tar.lz4 will go.
     * @param outputFileBaseName Output file base name, excluding the extension.  This wraps the source
     *                           under a new directory with this base name in the archive.
     * @return Path to the output file
     */
    public Path compress(String sourcePath, String destinationPath, String outputFileBaseName) {
        try {
            File sourceFile = new File(sourcePath);
            assert sourceFile.exists() && sourceFile.isDirectory() : "source path [" + sourcePath + "] is not a valid directory";
            destinationPath += outputFileBaseName + TAR_LZ4_EXTENSION;
            
            // Get our file count
            long fileCount = TarLz4Util.fileCount(Path.of(sourcePath));
            log.debug("Compressing {} files from source={} to destination={}", fileCount, sourcePath, destinationPath);

            if (numThreads < 2) {
                // In the single-threaded case, we simply write directly to the final output file
                try (FileOutputStream outputFile = new FileOutputStream(destinationPath)) {
                    new TarLz4CompressTask(sourcePath, destinationPath, 0, fileCount, 0, 1, 
                            this.bufferSize, TarLz4Util.getDirectorySize(sourceFile.toPath()), shouldLogProgress, logProgressPercentInterval, outputFile).run();
                }
            } else {
                // Reuse futures array
                var futures = new Future[numThreads];
                
                // Archive + Compression tasks
                submitArchiveTasks(sourcePath, destinationPath, fileCount, futures);

                // At this point, we have all our .tmp files which are standalone .tar.lz4 compressed archives for each  slice
                // The .tmp files can't be opened themselves however, as they are a sliced part of the final output file.
                // Here, we can now merge all the .tmp files we created, into the single final output file
                // There are multiple ways to merge files into one, such as Streams, NIO2, Apache Commons, etc.
                // From other benchmarks online, the most efficient way to do this is via FileChannels, which can use the
                // underlying OS and data caches to copy files closer to the hardware, giving us the fastest results.

                // Another thing to make note of is, we NEED to make sure we are writing to the final output file in parallel
                // across the multiple threads, otherwise this merging of .tmp files becomes a bottleneck!
                // This is made possible with the AsynchronousFileChannel API, which allows for writing bytes directly into a file
                // at some specified offset position.

                mergeTmpArchives(destinationPath, futures);
            }

            log.debug("Finished compressing {} files from source={} to destination={}", fileCount, sourcePath, destinationPath);
            return Path.of(destinationPath);
        } catch (Exception e) {
            log.error("Could not lz4 compress source=[" + sourcePath + "] to destination=[" + destinationPath + "]", e);
            throw new RuntimeException(e);
        }
    }
    
    private void submitArchiveTasks(String sourcePath, String destinationPath, long fileCount, Future<?>[] futures) 
            throws IOException, ExecutionException, InterruptedException {
        // Get the file number intervals
        // TODO: Make it configurable to use file count vs this
        long[] fileNumIntervals = TarLz4Util.getFileCountIntervalsFromSize(Path.of(sourcePath), numThreads);
        long totalBytes = fileNumIntervals[fileNumIntervals.length - 1];
        
        // In the multithreaded use case, we'll spin up `numThreads` threads, each writing to its own temporary file
        TarLz4CompressTask[] tasks = new TarLz4CompressTask[numThreads];
        
        boolean success = false;

        try {
            for (int i = 0; i < numThreads; i++) {
                // Spin up a thread for each Runnable task

                // start of the slice is from our fileNumIntervals
                long start = fileNumIntervals[i];

                // end of the slice is 1 before the next fileNumInterval, or if we are on the last slice
                // we can simply set this to the fileCount to cover the rest of the files
                long end = i == numThreads - 1 ? fileCount : fileNumIntervals[i + 1];

                // Each Runnable task will be outputting to a temporary file, which is the same name as the output file except
                // suffixed with "_sliceNum.tmp"
                // TODO: Make this randomly generated string and validate it doesn't already exist
                FileOutputStream tmpOutputFile = new FileOutputStream(destinationPath + "_" + i + TMP_SUFFIX);

                TarLz4CompressTask runnable = new TarLz4CompressTask(sourcePath, destinationPath, start, end, i, numThreads,
                        bufferSize, totalBytes, false, logProgressPercentInterval, tmpOutputFile);

                // Save a reference to each Thread Future, and the Runnable, so we can properly close() or clean them up later
                futures[i] = executorService.submit(runnable);
                tasks[i] = runnable;
            }

            // Logging progress for multithreaded case, also waits for future to finish
            if (shouldLogProgress) {
                int currPercent;
                int prevPercent = 0;
                boolean isDone;
                do {
                    currPercent = 0;
                    isDone = true;
                    for (int i = 0; i < numThreads; i++) {
                        currPercent += tasks[i].getBytesProcessed();
                        isDone &= futures[i].isDone();
                    }
                    
                    currPercent = (int) (currPercent * 100 / totalBytes);
                    if (prevPercent / logProgressPercentInterval < currPercent / logProgressPercentInterval) {
                        log.info("TarLz4 compression progress: {}%", currPercent);
                    }
                    prevPercent = currPercent;
                    Thread.sleep(100);
                } while (!isDone && prevPercent < 100);
            }

            // Wait for all futures to finish
            for (int i = 0; i < numThreads; i++) {
                futures[i].get();
                tasks[i].fos.close();   // Clean up and close the .tmp file OutputStreams
            }
            success = true;
        } finally {
            // Safety check
            if (!success) {
                // Wait for all futures to finish
                for (int i = 0; i < numThreads; i++) {
                    futures[i].get();
                    tasks[i].fos.close();   // Clean up and close the .tmp file OutputStreams
                }
            }
        }
        
        log.debug("Finished compressing archive task for source={}, destination={}", sourcePath, destinationPath);
    }
    
    private void mergeTmpArchives(String destinationPath, Future<?>[] futures) throws IOException, ExecutionException, InterruptedException {
        FileInputStream[] tmpFiles = new FileInputStream[numThreads];
        FileChannel[] tmpChannels = new FileChannel[numThreads];
        long[] fileChannelOffsets = new long[numThreads];

        // This grabs a FileChannel to read for each .tmp file, and also calculates what all the fileChannel position offsets
        // we should use for each Thread, based on the size in bytes of each .tmp file
        for (int i = 0; i < numThreads; i++) {
            tmpFiles[i] = new FileInputStream(destinationPath + "_" + i + TMP_SUFFIX);
            tmpChannels[i] = tmpFiles[i].getChannel();
            if (i < numThreads - 1) {
                fileChannelOffsets[i + 1] = fileChannelOffsets[i] + tmpChannels[i].size();
            }
        }

        // Create an AsynchronousFileChannel for the final output `.tar.lz4` file
        // This channel is has the capability to WRITE to the file, or CREATE it if it doesn't yet exist
        AsynchronousFileChannel destChannel = AsynchronousFileChannel.open(Path.of(destinationPath), WRITE, CREATE);
        for (int i = 0; i < numThreads; i++) {
            int finalI = i;
            // Let's again spin up a thread for each .tmp file to write to its slice, or region in the final output file
            futures[i] = executorService.submit(() -> {
                try {
                    log.debug("Writing output region for slice {}", finalI);
                    String tmpFilePath = destinationPath + "_" + finalI + ".tmp";

                    // You can play around with the buffer size to optimize
                    ByteBuffer buf = ByteBuffer.allocate(bufferSize);

                    // Let's get our FileChannel which we opened earlier, for the .tmp file
                    FileChannel tmpChannel = tmpChannels[finalI];

                    // The position in the output file this thread will be writing to
                    long pos = fileChannelOffsets[finalI];
                    int read;
                    while ((read = tmpChannel.read(buf)) != -1) {
                        buf.flip();
                        // Write bytes from the .tmp file to the offset position in the final output file
                        destChannel.write(buf, pos).get();
                        // Update our position for this thread
                        pos += read;
                        buf.clear();
                    }

                    // Clean up everything, including deleting the .tmp file now that we finished writing it all to the
                    // final output file
                    tmpChannel.close();
                    tmpFiles[finalI].close();
                    Files.deleteIfExists(Path.of(tmpFilePath));
                    log.debug("Finished writing output region for slice {}", finalI);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        // Wait for all futures to finish
        for (int i = 0; i < numThreads; i++) {
            futures[i].get();
        }

        // Done!
        destChannel.close();
    }
}
