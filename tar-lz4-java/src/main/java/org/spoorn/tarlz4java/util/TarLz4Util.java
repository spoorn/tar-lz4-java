package org.spoorn.tarlz4java.util;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicLong;

public class TarLz4Util {

    public static long fileCount(Path path) throws IOException {
        return Files.walk(path)
                .filter(p -> !p.toFile().isDirectory())
                .count();
    }

    public static long getDirectorySize(Path path) throws IOException {
        AtomicLong size = new AtomicLong();

        // Ignores failed files, should be fine for our use case
        Files.walkFileTree(path, new SimpleFileVisitor<>() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                size.addAndGet(attrs.size());
                return FileVisitResult.CONTINUE;
            }
        });

        return size.get();
    }

    /**
     * Scans through a directory and finds the file count intervals, meaning the file number while walking through the
     * path, that split all the files evenly by size.  For balancing multi-threaded processing of a directory recursively. 
     *
     * @param path Path to process
     * @param numIntervals Number of intervals
     * @return long[] that holds the file number indexes to split at
     * @throws IOException If processing files fail
     */
    public static long[] getFileCountIntervalsFromSize(Path path, int numIntervals) throws IOException {
        long[] res = new long[numIntervals];
        // index of res, file count, current size, previous size
        long[] state = {1, 0, 0, 0};
        long sliceLength = getDirectorySize(path) / numIntervals;

        Files.walkFileTree(path, new SimpleFileVisitor<>() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (state[0] < res.length) {
                    state[2] += attrs.size();
                    if (state[3] / sliceLength < state[2] / sliceLength) {
                        res[(int) state[0]] = state[1];
                        state[0]++;
                    }
                    state[3] = state[2];
                    state[1]++;
                    return FileVisitResult.CONTINUE;
                } else {
                    return FileVisitResult.TERMINATE;
                }
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                state[1]++;
                return super.visitFileFailed(file, exc);
            }
        });
        return res;
    }
}