package org.spoorn.tarlz4java.core;

import lombok.Getter;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.logging.log4j.Logger;
import org.spoorn.tarlz4java.io.CustomTarArchiveOutputStream;
import org.spoorn.tarlz4java.logging.TarLz4Logger;
import org.spoorn.tarlz4java.logging.Verbosity;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Set;

public class TarLz4CompressTask implements Runnable {

    private static final Logger logger = org.apache.logging.log4j.LogManager.getLogger(TarLz4CompressTask.class);
    private final String sourcePath;  // target input path
    private final String destinationPath;  // destination output file i.e. the temporary file this thread will write to
    private final int slice;  // The slice we are looking at, indexed at 0
    private final int totalSlices;  // The total number of slices.  Used to know if we are on the last slice to write the Tar Archive footers
    private final int bufferSize;   // buffer size for copying files to the Tar Archive
    private final long totalBytes; // Total number of bytes in the sourcePath, for logging progress purposes
    private final boolean shouldLogProgress;  // True to log progress via a Logger, else false
    private final int logProgressPercentInterval;  // Percentage interval to log progress
    private final Verbosity verbosity;  // logging verbosity
    private final Set<String> excludeFiles;  // Exclude files from the task
    public final FileOutputStream fos;  // Output Stream to the file output for this task

    private final long start;    // inclusive
    private final long end;   // exclusive
    private int count;  // Current file number this task is processing
    @Getter
    private long bytesProcessed;
    private final TarLz4Logger log;

    public TarLz4CompressTask(String sourcePath, String destinationPath, long start, long end, int slice,
                              int totalSlices, int bufferSize, long totalBytes, boolean shouldLogProgress,
                              int logProgressPercentInterval, Verbosity verbosity, Set<String> excludeFiles, FileOutputStream fos) {
        this.sourcePath = sourcePath;
        this.destinationPath = destinationPath;
        this.slice = slice;
        this.totalSlices = totalSlices;
        this.bufferSize = bufferSize;
        this.totalBytes = totalBytes;
        this.shouldLogProgress = shouldLogProgress;
        this.logProgressPercentInterval = logProgressPercentInterval;
        this.verbosity = verbosity;
        this.excludeFiles = excludeFiles;
        this.fos = fos;

        this.start = start;
        this.end = end;
        this.count = 0;
        this.bytesProcessed = 0;
        this.log = new TarLz4Logger(logger, verbosity);
    }

    @Override
    public void run() {
        try (LZ4FrameOutputStream outputStream = new LZ4FrameOutputStream(this.fos);
             CustomTarArchiveOutputStream taos = new CustomTarArchiveOutputStream(outputStream, this.slice == this.totalSlices - 1)) {

            log.debug("Starting compression task for slice {} with start={}, end={}", this.slice, this.start, this.end - 1);
            addFilesToTar(sourcePath, "", taos);
            log.debug("Finished compressed archive for slice {}", this.slice);

            taos.finish();
        } catch (IOException e) {
            log.error("Could not lz4 compress source=[" + sourcePath + "] to destination=[" + destinationPath + "] for slice " + slice, e);
            throw new RuntimeException(e);
        }
    }

    // Base needed as we are branching off of a child directory, so the initial source will be the virtual "root" of the tar
    private void addFilesToTar(String path, String base, TarArchiveOutputStream taos) throws IOException {
        try {
            File file = new File(path);
            
            if (!this.excludeFiles.contains(file.getName())) {
                // If we are out of the bounds of our slice, skip
                // This could probably be optimized to not have to walk through the entire file tree again.
                // Instead, we could have cached the exact files each slice should handle.
                // It's a trade off between using more memory, or more processing steps
                if (file.isFile() && (count < this.start || count >= this.end)) {
                    count++;
                    return;
                }

                String entryName = base + file.getName();

                if (file.isFile()) {
                    // Write file content to archive
                    try (FileInputStream fis = new FileInputStream(file)) {
                        long prevBytesProcessed = this.bytesProcessed;
                        // Add the Tar Archive Entry
                        taos.putArchiveEntry(new TarArchiveEntry(file, entryName));
                        taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
                        this.bytesProcessed += IOUtils.copy(fis, taos, this.bufferSize);
                        taos.closeArchiveEntry();

                        // Logging progress for single-thread case
                        if (shouldLogProgress && this.totalSlices == 1) {
                            long prevPercent = prevBytesProcessed * 100 / totalBytes;
                            long currPercent = this.bytesProcessed * 100 / totalBytes;
                            int interval = logProgressPercentInterval;
                            if (prevPercent / interval < currPercent / interval) {
                                log.info("TarLz4 compression progress: {}%", currPercent);
                            }
                        }

                        count++;
                    }
                } else {
                    // Add the Tar Archive Entry
                    taos.putArchiveEntry(new TarArchiveEntry(file, entryName));
                    taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
                    taos.closeArchiveEntry();
                    for (File f : file.listFiles()) {
                        // Recurse on nested files/directories
                        addFilesToTar(f.getPath(), entryName + File.separator, taos);
                    }
                }
            } else {
                log.debug("Skipping file {}", path);
            }
        } catch (Exception e) {
            log.error("Error while adding file {} to Tar", path);
            throw e;
        }
    }
}
