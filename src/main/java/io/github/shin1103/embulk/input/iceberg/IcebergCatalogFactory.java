package io.github.shin1103.embulk.input.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.embulk.util.config.units.SchemaConfig;
import java.util.HashMap;
import java.util.Map;

public class IcebergCatalogFactory {
    private enum CatalogType {
        REST,
        JDBC
    }

    public static Catalog createCatalog(String catalogType, IcebergInputPlugin.PluginTask task) {
        try {
            CatalogType type = CatalogType.valueOf(catalogType.toUpperCase());

            switch (type) {
                case REST:
                    return createRestCatalog(task);
                case JDBC:
                    throw new UnsupportedOperationException("JDBC is not supported");
                default:
                    throw new UnsupportedOperationException("");
            }
        } catch (IllegalArgumentException e) {
            throw new UnsupportedOperationException("Invalid value: " + catalogType);
        }
    }

    private static RESTCatalog createRestCatalog(IcebergInputPlugin.PluginTask task) {
        Map<String, String> properties = new HashMap<>();

        properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
        properties.put(CatalogProperties.URI, task.getUri());
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, task.getWarehouseLocation());
        properties.put(CatalogProperties.FILE_IO_IMPL, task.getFileIoImpl());
        properties.put(S3FileIOProperties.ENDPOINT, task.getEndpoint());
        // https://github.com/apache/iceberg/issues/7709
        properties.put(S3FileIOProperties.PATH_STYLE_ACCESS, task.getPathStyleAccess());
        // REST Catalog can read data using http protocol.
        // But S3FileIO Library need to REGION and ACCESS_KEY info using Environment variable

        RESTCatalog catalog = new RESTCatalog();
        Configuration conf = new Configuration();
        catalog.setConf(conf);
        catalog.initialize("internal_embulk_catalog", properties);
        return catalog;
    }

}
