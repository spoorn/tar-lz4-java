# tar-lz4-java
Java library for creating Tar Archives compressed with LZ4.  

This builds on top of the [lz4-java](https://github.com/lz4/lz4-java) library, providing convenience and extremely simple APIs to create `.tar.lz4` compressed archives from files and directories, abstracting the nuances of working with underlying IOStreams from the lz4-java library  and Apache Commons Compress.

Also adds multi-threaded support for compression!

