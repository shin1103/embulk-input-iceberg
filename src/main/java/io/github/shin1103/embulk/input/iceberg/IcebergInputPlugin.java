package io.github.shin1103.embulk.input.iceberg;

import io.github.shin1103.embulk.util.ClassLoaderSwap;

import java.math.BigDecimal;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;

import org.embulk.util.config.*;

import java.io.IOException;
import java.time.*;
import java.util.List;
import java.util.Optional;

import org.embulk.util.config.modules.ZoneIdModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Official Developer Guild
// https://docs.google.com/document/d/1oKpvgstKlgmgUUja8hYqTqWxtwsgIbONoUaEj8lO0FE/edit?pli=1&tab=t.0
// https://dev.embulk.org/topics/get-ready-for-v0.11-and-v1.0-updated.html

public class IcebergInputPlugin implements InputPlugin {

    private static final Logger logger = LoggerFactory.getLogger(IcebergInputPlugin.class);

    protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
            ConfigMapperFactory.builder().addDefaultModules().addModule(ZoneIdModule.withLegacyNames()).build();

    protected static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    protected static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();

    protected Class<? extends PluginTask> getTaskClass()
    {
        return PluginTask.class;
    }

    public interface PluginTask extends Task
    {
        @Config("namespace")
        @ConfigDefault("null")
        /*
          Catalog Namespace.
         */
        String getNamespace();

        @Config("table")
        @ConfigDefault("null")
        /*
          Catalog Namespace.
         */
        String getTable();

        @Config("catalog_type")
        @ConfigDefault("null")
        /*
          Now only REST catalog is supported.
         */
        String getCatalogType();


        @Config("uri")
        @ConfigDefault("null")
        /*
          Example
          REST: http://localhost:8181
         */
        Optional<String> getUri();

        @Config("warehouse_location")
        @ConfigDefault("null")
        /*
          root warehouse location
          Example
          s3://warehouse/
         */
        String getWarehouseLocation();

        @Config("file_io_impl")
        @ConfigDefault("null")
        /*
          io class to read and write warehouse
          Example
          org.apache.iceberg.aws.s3.S3FileIO
         */
        String getFileIoImpl();

        @Config("endpoint")
        @ConfigDefault("null")
        /*
          Object Storage Endpoint
          Example
          http://localhost:9000/
         */
        Optional<String> getEndpoint();

        @Config("path_style_access")
        @ConfigDefault("true")
        /*
          use path_style_access.
          If you use Example settings, actual path is "http://localhost:9000/warehouse/".
         */
        Optional<String> getPathStyleAccess();

        @Config("decimal_as_string")
        @ConfigDefault("false")
        /*
          Embulk can't treat Bigdecimal. If you want to treat precise, treat as string.
         */
        boolean getDecimalAsString();

        @Config("table_filters")
        @ConfigDefault("null")
        Optional<List<IcebergFilterOption>> getTableFilters();

        @Config("columns")
        @ConfigDefault("null")
        Optional<List<String>> getColumns();
    }

    @Override
    public ConfigDiff transaction(ConfigSource configSource, Control control) {
        try (ClassLoaderSwap<? extends IcebergInputPlugin> ignored = new ClassLoaderSwap<>(this.getClass())) {
            final PluginTask task = CONFIG_MAPPER.map(configSource, this.getTaskClass());

            Table table = this.get_table(task);

            Schema schema = this.createEmbulkSchema(table.schema(), task);
            return resume(task.toTaskSource(), schema, 1, control);
        }
    }

    private Table get_table(PluginTask task) {
        Catalog catalog = IcebergCatalogFactory.createCatalog(task.getCatalogType(), task);
        Namespace n_space = Namespace.of(task.getNamespace());
        TableIdentifier name = TableIdentifier.of(n_space, task.getTable());
        Table table = catalog.loadTable(name);
        logger.debug(table.schemas().toString());

        return table;
    }

    private Schema createEmbulkSchema(org.apache.iceberg.Schema icebergSchema, PluginTask task){
        Schema.Builder schemaBuilder = Schema.builder();

        for (Types.NestedField col : icebergSchema.columns()){
            if (task.getColumns().isPresent()) {
                if (task.getColumns().get().contains(col.name())) {
                    // only add column defined columns option in config.yml
                    schemaBuilder.add(col.name(), TypeConverter.convertIcebergTypeToEmbulkType(col.type(), task));
                } else {
                    continue;
                }
            } else {
                // add all columns if columns option is not defined in config.yml
                schemaBuilder.add(col.name(), TypeConverter.convertIcebergTypeToEmbulkType(col.type(), task));
            }
        }
        return schemaBuilder.build();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, Control control) {
        // Thread.currentThread().getContextClassLoader() is used in org.apache.iceberg.common.DynMethods.
        // If this swap is not executed, classLoader is not work collect.
        try (ClassLoaderSwap<? extends IcebergInputPlugin> ignored = new ClassLoaderSwap<>(this.getClass())) {
            control.run(taskSource, schema, taskCount);
        }

        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource, Schema schema, int i, List<TaskReport> list) {
    }

    @Override
    public TaskReport run(TaskSource taskSource, Schema schema, int i, PageOutput pageOutput) {
        final PluginTask task = TASK_MAPPER.map(taskSource, this.getTaskClass());

        BufferAllocator allocator = Exec.getBufferAllocator();
        PageBuilder pageBuilder = Exec.getPageBuilder(allocator, schema, pageOutput);

        Table table = this.get_table(task);
        try(CloseableIterable<Record> scan = IcebergScanBuilder.createBuilder(table, task).build()){
            for (Record data : scan) {
                schema.visitColumns(new ColumnVisitor() {
                    @Override
                    public void booleanColumn(Column column) {
                        if (data.getField(column.getName()) == null) {
                            pageBuilder.setNull(column);
                            return;
                        }
                        pageBuilder.setBoolean(column, (Boolean) data.getField(column.getName()));
                    }

                    @Override
                    public void longColumn(Column column) {
                        if (data.getField(column.getName()) == null) {
                            pageBuilder.setNull(column);
                            return;
                        }
                        if (data.getField(column.getName()).getClass() == Long.class) {
                            pageBuilder.setLong(column, (Long) data.getField(column.getName()));
                        } else {
                            pageBuilder.setLong(column, ((Integer) data.getField(column.getName())).longValue());
                        }
                    }

                    @Override
                    public void doubleColumn(Column column) {
                        if (data.getField(column.getName()) == null) {
                            pageBuilder.setNull(column);
                            return;
                        }
                        if (data.getField(column.getName()).getClass() == BigDecimal.class) {
                            pageBuilder.setDouble(column, ((BigDecimal) data.getField(column.getName())).doubleValue());
                        } else if (data.getField(column.getName()).getClass() == Float.class) {
                            pageBuilder.setDouble(column, ((Float) data.getField(column.getName())).doubleValue());
                        } else {
                            pageBuilder.setDouble(column, (Double) data.getField(column.getName()));
                        }
                    }

                    @Override
                    public void stringColumn(Column column) {
                        if (data.getField(column.getName()) == null) {
                            pageBuilder.setNull(column);
                            return;
                        }
                        if (data.getField(column.getName()).getClass() == LocalTime.class) {
                            pageBuilder.setString(column, ((LocalTime) data.getField(column.getName())).toString());
                        } else if (data.getField(column.getName()).getClass() == BigDecimal.class) {
                            pageBuilder.setString(column, ((BigDecimal) data.getField(column.getName())).toPlainString());
                        } else {
                            pageBuilder.setString(column, (String) data.getField(column.getName()));
                        }
                    }

                    @Override
                    public void timestampColumn(Column column) {
                        if (data.getField(column.getName()) == null) {
                            pageBuilder.setNull(column);
                            return;
                        }
                        if (data.getField(column.getName()).getClass() == LocalDate.class) {
                            pageBuilder.setTimestamp(column, ((LocalDate) data.getField(column.getName())).atStartOfDay(ZoneId.systemDefault()).toInstant());
                        } else if (data.getField(column.getName()).getClass() == LocalDateTime.class) {
                            pageBuilder.setTimestamp(column, ((LocalDateTime) data.getField(column.getName())).atZone(ZoneId.systemDefault()).toInstant());
                        } else {
                            pageBuilder.setTimestamp(column, ((OffsetDateTime) data.getField(column.getName())).toInstant());
                        }
                    }

                    @Override
                    public void jsonColumn(Column column) {
                        throw new NotImplementedException("JSON Type is not supported");
                    }
                });

                pageBuilder.addRecord();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        pageBuilder.finish();
        pageBuilder.close();
        return CONFIG_MAPPER_FACTORY.newTaskReport();
    }

    @Override
    public ConfigDiff guess(ConfigSource configSource)
    {
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }
}
