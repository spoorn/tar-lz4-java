# tar-lz4-java
Java library for creating Tar Archives compressed with LZ4.  

This builds on top of the [lz4-java](https://github.com/lz4/lz4-java) library, providing convenience and extremely simple APIs to create `.tar.lz4` compressed archives from files and directories, abstracting the nuances of working with underlying IOStreams from the lz4-java library  and Apache Commons Compress.

Also adds multi-threaded support for compression!

Note: This currently only supports Tar Archive + Compressing directories.  Singular files can be done directly through LZ4 without the need for Tar.  Support for single files is TBD

# How to Install

### Available on Maven Central

TBD

---

### Using <u>Jitpack.io</u>

#### build.gradle:

```groovy
respositories {
  ...
  maven { url "https://jitpack.io" }
}

dependencies {
  ...
  implementation("com.github.spoorn:tar-lz4-java:<version>")
}
```

_or_

### maven:

```xml
<repositories>
  <repository>
      <id>jitpack.io</id>
      <url>https://jitpack.io</url>
  </repository>
</repositories>

<dependency>
    <groupId>com.github.spoorn</groupId>
    <artifactId>tar-lz4-java</artifactId>
    <version>see Releases page for version</version>
</dependency>
```

You can find the `<version>` under [Releases](https://github.com/spoorn/tar-lz4-java/releases)

---

### If you are using Java9+ Modules

Module name: `org.spoorn.tarlz4java`

__Add to your module-info.java__

```java
module <your-module-name> {
  ...
  requires static org.spoorn.tarlz4java;
}
```

# How to Use

All classes should have rich documentation to accompany it.  Here's a quick overview with some examples.

## Compression

1. Use `TarLz4CompressorBuilder` to configure the compressor (all configurations are optional, you can see defaults [here](https://github.com/spoorn/tar-lz4-java/blob/main/tar-lz4-java/src/main/java/org/spoorn/tarlz4java/api/TarLz4CompressorBuilder.java#L13)) and build the `TarLz4Compressor` object
2. `TarLz4Compressor` is used to compress from some sourcePath to a destinationPath.  You can optionally add a custom output file name for the `.tar.lz4`

```java
// Simple without configurations
TarLz4Compressor simpleCompressor = new TarLz4CompressorBuilder().build();
simpleCompressor.compress(sourcePath, destinationPath);

// With configurations
TarLz4Compressor compressor = new TarLz4CompressorBuilder()
        .numThreads(4)
        .bufferSize(8192)
        .logProgressPercentInterval(10)
        .executorService(Executors.newFixedThreadPool(numThreads, new NamedThreadFactory("MyThreadPool")))
        .shouldLogProgress(true)
        .verbosity(Verbosity.DEBUG)
        .excludeFiles(Set.of("donotcompress.lock"))
        .build();
compressor.compress(sourcePath, destinationPath, "customoutputfilename");
```

Note: `sourcePath` should be the full path to a directory or file.  `destinationPath` should be the path to a directory where the compressed archive will be outputed to.

## Decompression

1. Use `TarLz4DecompressorBuilder` to configure the decompressor (all configurations are optional, you can see defaults [here](https://github.com/spoorn/tar-lz4-java/blob/main/tar-lz4-java/src/main/java/org/spoorn/tarlz4java/api/TarLz4DecompressorBuilder.java#L10)) and build the `TarLz4Decompressor` object
2. `TarLz4Decompressor` is used to decompress from some sourcePath to a destinationPath

```java
// Simple without configurations
TarLz4Decompressor simpleDecompressor = new TarLz4DecompressorBuilder().build();
simpleDecompressor.decompress(sourcePath, destinationPath);

// With configurations
TarLz4Decompressor decompressor = new TarLz4DecompressorBuilder()
        .logProgressPercentInterval(10)
        .shouldLogProgress(true)
        .verbosity(Verbosity.DEBUG)
        .build();
decompressor.decompress(sourcePath, destinationPath);
```

Note: `sourcePath` should be the full path to a `.tar.lz4` file.  `destinationPath` should be the path to a directory where the decompressed extracted files will be outputed to.

## Shared

Various configurations are common between the compressor/decompressor.  All should be documented fully in the javadoc which is the official reference and source of truth for documentation.


# Technical Details

I originally created this library to help build a backup mod for Minecraft world folders.  See https://github.com/spoorn/tar-lz4-java/blob/main/SUMMARY.md for technical details and a single-point benchmark xD
