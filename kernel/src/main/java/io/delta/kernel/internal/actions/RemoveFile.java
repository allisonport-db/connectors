package io.delta.kernel.internal.actions;

import java.util.Map;
import java.util.Optional;

import io.delta.kernel.data.Row;
import io.delta.kernel.fs.Path;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

public class RemoveFile extends FileAction {

    public static RemoveFile fromRow(Row row) {
        if (row == null) return null;

        final String path = row.getString(0);
        final long deletionTimestamp = row.getLong(1);
        final Map<String, String> partitionValues = row.getMap(2);
        final long size = row.getLong(3);
        final boolean dataChange = row.getBoolean(4);
        final DeletionVectorDescriptor deletionVector =
                DeletionVectorDescriptor.fromRow(row.getRecord(5));

        return new RemoveFile(
                path,
                deletionTimestamp,
                partitionValues,
                size,
                dataChange,
                deletionVector);
    }

    public static final StructType READ_SCHEMA = new StructType()
            .add("path", StringType.INSTANCE)
            .add("deletionTimestamp", LongType.INSTANCE)
            .add("partitionValues", new MapType(StringType.INSTANCE, StringType.INSTANCE, false))
            .add("size", LongType.INSTANCE)
            .add("dataChange", BooleanType.INSTANCE)
            .add("deletionVector", DeletionVectorDescriptor.READ_SCHEMA);

    private final long deletionTimestamp;
    private final Map<String, String> partitionValues;
    private final long size;

    public RemoveFile(
            String path,
            long deletionTimestamp,
            Map<String, String> partitionValues,
            long size,
            boolean dataChange,
            DeletionVectorDescriptor deletionVector) {
        super(path, dataChange, deletionVector);

        this.deletionTimestamp = deletionTimestamp;
        this.partitionValues = partitionValues;
        this.size = size;
    }

    @Override
    public RemoveFile copyWithDataChange(boolean dataChange) {
         return new RemoveFile(
                 this.path,
                 this.deletionTimestamp,
                 this.partitionValues,
                 this.size,
                 dataChange,
                 this.deletionVector
         );
    }

    public RemoveFile withAbsolutePath(Path dataPath) {
        Path filePath = new Path(path);
        if (filePath.isAbsolute()) {
            return this;
        }
        Path absPath = new Path(dataPath, filePath);
        return new RemoveFile(
                absPath.toString(),
                this.deletionTimestamp,
                this.partitionValues,
                this.size,
                this.dataChange,
                this.deletionVector
        );
    }
}
