package io.github.shin1103.embulk.input.iceberg;

import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

public class TypeConverter
{
    private TypeConverter() {}

    /*
        https://iceberg.apache.org/spec/#primitive-types
     */
    public static Type convertIcebergTypeToEmbulkType(org.apache.iceberg.types.Type icebergType, IcebergInputPlugin.PluginTask task)
    {

        switch (icebergType.typeId()) {
            case BOOLEAN:
                return Types.BOOLEAN;
            case INTEGER:
            case LONG:
                return Types.LONG;
            case FLOAT:
            case DOUBLE:
                return Types.DOUBLE;
            case DECIMAL:
                if (task.getDecimalAsString()) {
                    return Types.STRING;
                }
                else {
                    return Types.DOUBLE;
                }
            case DATE:
            case TIMESTAMP:
                return Types.TIMESTAMP;
            case STRING:
            case TIME:
            case UUID:
                return Types.STRING;
            case TIMESTAMP_NANO: // Support Iceberg v3
            case UNKNOWN:
            case FIXED:
            case BINARY:
            case STRUCT:
            case LIST:
            case MAP:
            case VARIANT:
                throw new UnsupportedOperationException(icebergType.typeId() + " is not supported");
            default:
                throw new IllegalArgumentException();
        }
    }
}
