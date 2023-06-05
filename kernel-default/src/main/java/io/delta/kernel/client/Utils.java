package io.delta.kernel.client;

import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.List;

public class Utils
{
    private Utils() {
    }

    /**
     * Given the file schema in Parquet file and selected columns by Delta, return
     * a subschema of the file schema.
     * @param fileSchema
     * @param deltaType
     * @return
     */
    public static final MessageType pruneSchema(
            MessageType fileSchema, // parquet
            StructType deltaType) // delta-core
    {
        // TODO: Handle the case where the column is not in Parquet file
        return deltaType.fields().stream()
                .map(column -> {
                    Type type = findStructField(fileSchema, column);
                    if (type == null) {
                        return null;
                    }
                    Type prunedSubfields = pruneSubfields(type, column.getDataType());
                    return new MessageType(column.getName(), prunedSubfields);
                })
                .filter(type -> type != null)
                .reduce(MessageType::union)
                .get();
    }

    private static Type findStructField(MessageType fileSchema, StructField column)
    {
        // TODO: we need to provide a way to search by id.
        final String columnName = column.getName();
        if (fileSchema.containsField(columnName)) {
            return fileSchema.getType(columnName);
        }
        // parquet is case-sensitive, but hive is not. all hive columns get converted to lowercase
        // check for direct match above but if no match found, try case-insensitive match
        for (org.apache.parquet.schema.Type type : fileSchema.getFields()) {
            if (type.getName().equalsIgnoreCase(columnName)) {
                return type;
            }
        }

        // Create a type and return.
        return parquetTypeFromDeltaType(column.getDataType());
    }

    private static Type pruneSubfields(Type type, DataType deltaDatatype) {
        if (!(deltaDatatype instanceof StructType)) {
            // there is no pruning for non-struct types
            return type;
        }

        GroupType groupType = (GroupType) type;
        StructType deltaStructType = (StructType) deltaDatatype;
        List<Type> newParquetSubFields = new ArrayList<>();
        for (StructField subField : deltaStructType.fields()) {
            String subFieldName = subField.getName();
            Type parquetSubFieldType = groupType.getType(subFieldName);
            if (parquetSubFieldType == null) {
                for (org.apache.parquet.schema.Type typeTemp : groupType.getFields()) {
                    if (typeTemp.getName().equalsIgnoreCase(subFieldName)) {
                        parquetSubFieldType = type;
                    }
                }
            }
            newParquetSubFields.add(parquetSubFieldType);
        }
        return groupType.withNewFields(newParquetSubFields);
    }

    private static Type parquetTypeFromDeltaType(DataType deltaType) {
//        if (deltaType.typeName().equalsIgnoreCase(IntegerType.INSTANCE.typeName())) {
//            return PrimitiveType
//        }
        return null;
    }
}
