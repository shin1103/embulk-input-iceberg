package io.github.shin1103.embulk.input.iceberg;

import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

public class TypeConverter {

    /*
        https://iceberg.apache.org/spec/#primitive-types
     */
    public static Type convertIcebergTypeToEmbulkType(org.apache.iceberg.types.Type icebergType){
        switch (icebergType.typeId()){
            case BOOLEAN:
                return Types.BOOLEAN;
            case INTEGER:
            case LONG:
                return Types.LONG;
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return Types.DOUBLE;
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_NANO:
                return Types.TIMESTAMP;
            case STRING:
            case UNKNOWN:
                return Types.STRING;
            case UUID:
            case FIXED:
            case BINARY:
            case STRUCT:
            case LIST:
            case MAP:
            case VARIANT:
                throw new UnsupportedOperationException(icebergType.typeId() +  " is not supported");
            default:
                throw new IllegalArgumentException();
        }
    }
}
