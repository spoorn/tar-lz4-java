package org.spoorn.tarlz4java.core;

import lombok.extern.log4j.Log4j2;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.spoorn.tarlz4java.io.CustomTarArchiveOutputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

@Log4j2
public class TarLz4CompressTask implements Runnable {

    private final String sourcePath;  // target input path
    private final String destinationPath;  // destination output file i.e. the temporary file this thread will write to
    private final int slice;  // The slice we are looking at, indexed at 0
    private final int totalSlices;  // The total number of slices.  Used to know if we are on the last slice to write the Tar Archive footers
    private final int bufferSize;   // buffer size for copying files to the Tar Archive
    public final FileOutputStream fos;  // Output Stream to the file output for this task

    private final long start;    // inclusive
    private final long end;   // exclusive
    private int count;  // Current file number this task is processing

    public TarLz4CompressTask(String sourcePath, String destinationPath, long start, long end, int slice,
                              int totalSlices, int bufferSize, FileOutputStream fos) {
        this.sourcePath = sourcePath;
        this.destinationPath = destinationPath;
        this.slice = slice;
        this.totalSlices = totalSlices;
        this.bufferSize = bufferSize;
        this.fos = fos;

        this.start = start;
        this.end = end;
        this.count = 0;
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
        File file = new File(path);

        // If we are out of the bounds of our slice, skip
        // This could probably be optimized to not have to walk through the entire file tree again.
        // Instead, we could have cached the exact files each slice should handle.
        // It's a trade off between using more memory, or more processing steps
        if (file.isFile() && (count < this.start || count >= this.end)) {
            count++;
            return;
        }

        String entryName = base + file.getName();

        // Add the Tar Archive Entry
        taos.putArchiveEntry(new TarArchiveEntry(file, entryName));
        taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);

        if (file.isFile()) {
            // Write file content to archive
            try (FileInputStream fis = new FileInputStream(file)) {
                IOUtils.copy(fis, taos, this.bufferSize);
                taos.closeArchiveEntry();

                count++;
            }
        } else {
            taos.closeArchiveEntry();
            for (File f : file.listFiles()) {
                // Recurse on nested files/directories
                addFilesToTar(f.getPath(), entryName + File.separator, taos);
            }
        }
    }
}
