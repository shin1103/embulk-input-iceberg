package io.github.shin1103.embulk.input.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.RESTCatalog;

import java.util.HashMap;
import java.util.Map;

public class IcebergCatalogFactory
{
    private IcebergCatalogFactory()
    {
    }

    public enum CatalogType
    {
        REST,
        JDBC,
        GLUE
    }

    public static Catalog createCatalog(String catalogType, IcebergInputPlugin.PluginTask task)
    {
        try {
            CatalogType type = CatalogType.valueOf(catalogType.toUpperCase());

            switch (type) {
                case REST:
                    return createRestCatalog(task);
                case JDBC:
                    return createJdbcCatalog(task);
                case GLUE:
                    return createGlueCatalog(task);
                default:
                    throw new UnsupportedOperationException("");
            }
        }
        catch (IllegalArgumentException e) {
            throw new UnsupportedOperationException("Invalid value: " + catalogType);
        }
    }

    private static RESTCatalog createRestCatalog(IcebergInputPlugin.PluginTask task)
    {
        Map<String, String> properties = new HashMap<>();

        properties.put(CatalogProperties.CATALOG_IMPL, RESTCatalog.class.getName());
        task.getUri().ifPresent(uri -> properties.put(CatalogProperties.URI, uri));
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, task.getWarehouseLocation());
        properties.put(CatalogProperties.FILE_IO_IMPL, task.getFileIoImpl());
        task.getEndpoint().ifPresent(endpoint -> properties.put(S3FileIOProperties.ENDPOINT, endpoint));
        // https://github.com/apache/iceberg/issues/7709
        task.getPathStyleAccess().ifPresent(access -> properties.put(S3FileIOProperties.PATH_STYLE_ACCESS, access));
        // REST Catalog can read data using http protocol.
        // But S3FileIO Library needs to REGION and ACCESS_KEY info using Environment variable

        RESTCatalog catalog = new RESTCatalog();
        Configuration conf = new Configuration();
        catalog.setConf(conf);
        task.getCatalogName().ifPresent(catalogName -> catalog.initialize(catalogName, properties));
        return catalog;
    }

    private static GlueCatalog createGlueCatalog(IcebergInputPlugin.PluginTask task)
    {
        Map<String, String> properties = new HashMap<>();

        properties.put(CatalogProperties.CATALOG_IMPL, GlueCatalog.class.getName());
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, task.getWarehouseLocation());
        properties.put(CatalogProperties.FILE_IO_IMPL, task.getFileIoImpl());
        // S3FileIO Library need to REGION and ACCESS_KEY info using Environment variable

        GlueCatalog catalog = new GlueCatalog();
        Configuration conf = new Configuration();
        catalog.setConf(conf);

        task.getCatalogName().ifPresent(catalogName -> catalog.initialize(catalogName, properties));
        return catalog;
    }

    private static JdbcCatalog createJdbcCatalog(IcebergInputPlugin.PluginTask task)
    {
        Map<String, String> properties = new HashMap<>();

        properties.put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, task.getWarehouseLocation());
        properties.put(CatalogProperties.FILE_IO_IMPL, task.getFileIoImpl());

        task.getEndpoint().ifPresent(endpoint -> properties.put(S3FileIOProperties.ENDPOINT, endpoint));
        // https://github.com/apache/iceberg/issues/7709
        task.getPathStyleAccess().ifPresent(access -> properties.put(S3FileIOProperties.PATH_STYLE_ACCESS, access));

        task.getUri().ifPresent(uri -> properties.put(CatalogProperties.URI, uri));
        task.getJdbcUser().ifPresent(user -> properties.put(JdbcCatalog.PROPERTY_PREFIX + "user", user));
        task.getJdbcPass().ifPresent(pass -> properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", pass));

        JdbcCatalog catalog = new JdbcCatalog();
        Configuration conf = new Configuration();
        catalog.setConf(conf);
        task.getCatalogName().ifPresent(catalogName -> catalog.initialize(catalogName, properties));
        return catalog;
    }
}
