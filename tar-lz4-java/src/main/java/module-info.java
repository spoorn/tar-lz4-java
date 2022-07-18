module org.spoorn.tarlz4java {
    requires static org.lz4.java;
    requires static org.apache.commons.compress;
    requires static lombok;
    requires static org.apache.logging.log4j;
    requires static org.apache.logging.log4j.core;
    exports org.spoorn.tarlz4java.api;
    exports org.spoorn.tarlz4java.logging;
    exports org.spoorn.tarlz4java.util.concurrent;
}