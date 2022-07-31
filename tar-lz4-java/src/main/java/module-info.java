module org.spoorn.tarlz4java {
    requires org.lz4.java;
    requires org.apache.commons.compress;
    requires static lombok;
    requires transitive org.apache.logging.log4j;
    requires org.apache.logging.log4j.core;
    exports org.spoorn.tarlz4java.api;
    exports org.spoorn.tarlz4java.logging;
    exports org.spoorn.tarlz4java.util.concurrent;
}