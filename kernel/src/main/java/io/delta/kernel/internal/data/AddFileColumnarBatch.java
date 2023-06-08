package io.delta.kernel.internal.data;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.internal.actions.AddFile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Expose the array of {@link AddFile}s as a {@link ColumnarBatch}.
 */
public class AddFileColumnarBatch
        extends PojoColumnarBatch
{
    private static final Map<Integer, Function<AddFile, Object>> ordinalToAccessor = new HashMap<>();
    private static final Map<Integer, String> ordinalToColName = new HashMap<>();

    static {
        ordinalToAccessor.put(0, (a) -> a.getPath());
        ordinalToAccessor.put(1, (a) -> a.getPartitionValues());
        ordinalToAccessor.put(2, (a) -> a.getSize());
        ordinalToAccessor.put(3, (a) -> a.getModificationTime());
        ordinalToAccessor.put(4, (a) -> a.isDataChange());
        ordinalToAccessor.put(5, (a) -> a.getDeletionVectorAsRow());

        ordinalToColName.put(0, "path");
        ordinalToColName.put(1, "partitionValues");
        ordinalToColName.put(2, "size");
        ordinalToColName.put(3, "modificationTime");
        ordinalToColName.put(4, "dataChange");
        ordinalToColName.put(5, "deletionVector");
    }

    public AddFileColumnarBatch(List<AddFile> addFiles)
    {
        super(
                requireNonNull(addFiles, "addFiles is null"),
                AddFile.READ_SCHEMA,
                ordinalToAccessor,
                ordinalToColName);
    }
}
