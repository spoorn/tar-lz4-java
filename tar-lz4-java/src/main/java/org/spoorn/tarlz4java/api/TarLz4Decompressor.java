package org.spoorn.tarlz4java.api;

import static org.spoorn.tarlz4java.api.TarLz4Compressor.TAR_LZ4_EXTENSION;
import lombok.extern.log4j.Log4j2;
import net.jpountz.lz4.LZ4FrameInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.spoorn.tarlz4java.util.TarLz4Util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

@Log4j2
public class TarLz4Decompressor {

    private final boolean shouldLogProgress;
    private final int logProgressPercentInterval;
    
    public TarLz4Decompressor(boolean shouldLogProgress, int logProgressPercentInterval) {
        this.shouldLogProgress = shouldLogProgress;
        this.logProgressPercentInterval = logProgressPercentInterval;
    }

    /**
     * Allows setting the global log level for TarLz4Decompressor instances.
     * This is useful for enabling debug logs by calling <code>TarLz4Decompressor.setGlobalLogLevel(Level.DEBUG);</code>
     *
     * @param level Log level to set to
     */
    public static void setGlobalLogLevel(Level level) {
        Configurator.setLevel(log.getName(), level);
    }

    /**
     * Decompresses/extracts a .tar.lz4 compressed archive.
     *
     * @param sourcePath Path to .tar.lz4 file to decompress
     * @param destinationPath Path to a destination directory to put the extracted files in
     * @return Path to the destination file/directory that was decompressed
     */
    public Path decompress(Path sourcePath, Path destinationPath) {
        return decompress(sourcePath.toString(), destinationPath.toString());
    }

    /**
     * Decompresses/extracts a .tar.lz4 compressed archive.
     * 
     * @param sourcePath Path to .tar.lz4 file to decompress
     * @param destinationPath Path to a destination directory to put the extracted files in
     * @return Path to the destination file/directory that was decompressed
     */
    public Path decompress(String sourcePath, String destinationPath) {
        try {
            File sourceFile = new File(sourcePath);
            assert sourceFile.exists() && sourceFile.isFile() && sourceFile.getName().endsWith(TAR_LZ4_EXTENSION) 
                    : "source path [" + sourcePath + "] is not a valid .tar.lz4";
            String sourceFileName = sourceFile.getName();
            String sourceBaseName = sourceFileName.substring(0, sourceFileName.lastIndexOf(TAR_LZ4_EXTENSION));
            destinationPath += sourceBaseName;

            log.debug("Decompressing archive from source={} to destination={}", sourcePath, destinationPath);

            TarArchiveEntry entry = null;
            try (FileInputStream fis = new FileInputStream(sourceFile);
                 LZ4FrameInputStream lz4FrameInputStream = new LZ4FrameInputStream(fis);
                 TarArchiveInputStream tais = new TarArchiveInputStream(lz4FrameInputStream)) {
                
                long totalBytes = sourceFile.length();
                long bytesProcessed = 0;
                
                while ((entry = tais.getNextTarEntry()) != null) {
                    Path dest = Path.of(destinationPath, entry.getName());
                    
                    if (entry.isDirectory()) {
                        Files.createDirectories(dest);
                    } else {
                        try (FileOutputStream fos = new FileOutputStream(dest.toString())) {
                            byte[] content = new byte[(int) entry.getSize()];
                            int read = 0;
                            while (read < entry.getSize()) {
                                read += tais.read(content, read, content.length - read);
                            }
                            
                            if (this.shouldLogProgress) {
                                long prevBytesProcessed = bytesProcessed;
                                bytesProcessed += read;
                                int prevPercent = Math.min((int) (prevBytesProcessed * 100 / totalBytes), 100);
                                int currPercent = Math.min((int) ((bytesProcessed) * 100 / totalBytes), 100);
                                int interval = logProgressPercentInterval;
                                if (prevPercent / interval < currPercent / interval) {
                                    log.info("TarLz4 decompression progress: {}%", currPercent);
                                }
                            }

                            fos.write(content);
                        }
                    }
                }
            } catch (Exception e) {
                if (entry != null) {
                    log.error("Error decompressing Tar Archive Entry {}", entry.getName());
                }
                throw e;
            }

            Path res = Path.of(destinationPath);
            log.debug("Finished decompressing {} files from source={} to destination={}", TarLz4Util.fileCount(res), sourcePath, destinationPath);
            return res;
        } catch (Exception e) {
            log.error("Could not decompress source=[" + sourcePath + "] to destination=[" + destinationPath + "]", e);
            throw new RuntimeException(e);
        }
    }
}
