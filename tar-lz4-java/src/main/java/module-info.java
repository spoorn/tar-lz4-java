open module org.spoorn.tarlz4java {
    requires static org.lz4.java;
    requires static org.apache.commons.compress;
    requires static lombok;
    requires static org.apache.logging.log4j;
    exports org.spoorn.tarlz4java.api;
}