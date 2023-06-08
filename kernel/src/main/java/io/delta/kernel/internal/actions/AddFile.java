package io.delta.kernel.internal.actions;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import io.delta.kernel.data.Row;
import io.delta.kernel.fs.Path;
import io.delta.kernel.types.*;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

public class AddFile extends FileAction {

    ////////////////////////////////////////////////////////////////////////////////
    // Static Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    public static AddFile fromRow(Row row) {
        if (row == null) return null;

        final String path = row.getString(0);
        final Map<String, String> partitionValues = row.getMap(1);
        final long size = row.getLong(2);
        final long modificationTime = row.getLong(3);
        final boolean dataChange = row.getBoolean(4);
        final DeletionVectorDescriptor deletionVector =
                DeletionVectorDescriptor.fromRow(row.getRecord(5));

        return new AddFile(
                path, partitionValues, size, modificationTime, dataChange, deletionVector);
    }

    public static final StructType READ_SCHEMA = new StructType()
        .add("path", StringType.INSTANCE)
        .add("partitionValues", new MapType(StringType.INSTANCE, StringType.INSTANCE, false))
        .add("size", LongType.INSTANCE)
        .add("modificationTime", LongType.INSTANCE)
        .add("dataChange", BooleanType.INSTANCE)
        .add("deletionVector", DeletionVectorDescriptor.READ_SCHEMA);

    ////////////////////////////////////////////////////////////////////////////////
    // Instance Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    private final Map<String, String> partitionValues;
    private final long size;
    private final long modificationTime;
    private final DeletionVectorDescriptor deletionVector;

    public AddFile(
            String path,
            Map<String, String> partitionValues,
            long size,
            long modificationTime,
            boolean dataChange) {
        super(path, dataChange);

        if (partitionValues == null) {
            partitionValues = Collections.emptyMap();
        }
        this.partitionValues = partitionValues;
        this.size = size;
        this.modificationTime = modificationTime;
        this.deletionVector = null;
    }

    public AddFile(
            String path,
            Map<String, String> partitionValues,
            long size,
            long modificationTime,
            boolean dataChange,
            DeletionVectorDescriptor deletionVector) {
        super(path, dataChange);

        if (partitionValues == null) {
            partitionValues = Collections.emptyMap();
        }
        this.partitionValues = partitionValues;
        this.size = size;
        this.modificationTime = modificationTime;
        this.deletionVector = deletionVector;
    }

    @Override
    public AddFile copyWithDataChange(boolean dataChange) {
        return this; // TODO
    }

    public AddFile withAbsolutePath(Path dataPath) {
        Path filePath = new Path(path);
        if (filePath.isAbsolute()) {
            return this;
        }
        Path absPath = new Path(dataPath, filePath);
        return new AddFile(
                absPath.toString(),
                this.partitionValues,
                this.size,
                this.modificationTime,
                this.dataChange,
                this.deletionVector
        );
    }

    public Map<String, String> getPartitionValues() {
        return Collections.unmodifiableMap(partitionValues);
    }

    public Optional<String> getDeletionVectorUniqueId() {
        // TODO: IMPLEMENT THIS
        return Optional.empty();
    }

    public DeletionVectorDescriptor getDeletionVector() {
        return deletionVector;
    }

    public Row getDeletionVectorAsRow() {
        if (deletionVector == null) {
            return null;
        } else {
            return deletionVector.asRow();
        }
    }

    public long getSize() {
        return size;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    @Override
    public String toString() {
        return "AddFile{" +
            "path='" + path + '\'' +
            ", partitionValues=" + partitionValues +
            ", size=" + size +
            ", modificationTime=" + modificationTime +
            ", dataChange=" + dataChange +
            ", deletionVector=" + deletionVector +
            '}';
    }
}
