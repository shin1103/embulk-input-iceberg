package io.github.shin1103.embulk.input.iceberg;

import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.expressions.Expressions;

public class IcebergScanBuilder {

    public static IcebergGenerics.ScanBuilder createBuilder(Table table, IcebergInputPlugin.PluginTask task) {
        IcebergGenerics.ScanBuilder builder = IcebergGenerics.read(table);

        // determine select columns
        task.getColumns().ifPresent(builder::select);

        // determine select rows
        var filters = task.getTableFilters();
        if (filters.isPresent()) {
            for (IcebergFilterOption filter : filters.get()) {
                // support filter is predicate expressions only
                // https://iceberg.apache.org/docs/1.8.1/api/#expressions
                switch (filter.getFilterType().toUpperCase()) {
                    case "ISNULL":
                        builder.where(Expressions.isNull(filter.getColumn()));
                        continue;
                    case "NOTNULL":
                        builder.where(Expressions.notNull(filter.getColumn()));
                        continue;
                    case "EQUAL":
                        if (filter.getValue().isPresent()) {
                            builder.where(Expressions.equal(filter.getColumn(), filter.getValue().get()));
                        }
                        continue;
                    case "NOTEQUAL":
                        if (filter.getValue().isPresent()) {
                            builder.where(Expressions.notEqual(filter.getColumn(), filter.getValue().get()));
                        }
                        continue;
                    case "LESSTHAN":
                        if (filter.getValue().isPresent()) {
                            builder.where(Expressions.lessThan(filter.getColumn(), filter.getValue().get()));
                        }
                        continue;
                    case "LESSTHANOREQUAL":
                        if (filter.getValue().isPresent()) {
                            builder.where(Expressions.lessThanOrEqual(filter.getColumn(), filter.getValue().get()));
                        }
                        continue;
                    case "GREATERTHAN":
                        if (filter.getValue().isPresent()) {
                            builder.where(Expressions.greaterThan(filter.getColumn(), filter.getValue().get()));
                        }
                        continue;
                    case "GREATERTHANOREQUAL":
                        if (filter.getValue().isPresent()) {
                            builder.where(Expressions.greaterThanOrEqual(filter.getColumn(), filter.getValue().get()));
                        }
                        continue;
                    case "IN":
                        if (filter.getInValues().isPresent()) {
                            builder.where(Expressions.in(filter.getColumn(), filter.getInValues().get()));
                        }
                        continue;
                    case "NOTIN":
                        if (filter.getInValues().isPresent()) {
                            builder.where(Expressions.notIn(filter.getColumn(), filter.getInValues().get()));
                        }
                        continue;
                    case "STARTSWITH":
                        if (filter.getInValues().isPresent()) {
                            builder.where(Expressions.startsWith(filter.getColumn(), (String) filter.getValue().get()));
                        }
                        continue;
                    case "NOTSTARTSWITH":
                        if (filter.getValue().isPresent()) {
                            builder.where(Expressions.notStartsWith(filter.getColumn(), (String) filter.getValue().get()));
                        }
                        continue;
                    default:
                }

            }
        }
        return builder;
    }
}
