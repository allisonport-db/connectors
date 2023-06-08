package io.delta.kernel.internal.actions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.PojoRow;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import static java.util.Objects.requireNonNull;

/** Information about a deletion vector attached to a file action. */
public class DeletionVectorDescriptor {

    ////////////////////////////////////////////////////////////////////////////////
    // Static Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    public static DeletionVectorDescriptor fromRow(Row row) {
        if (row == null) return null;

        final String storageType = row.getString(0);
        final String pathOrInlineDv = row.getString(1);
        final int offset = row.getInt(2); // TODO could be null?!
        final int sizeInBytes = row.getInt(3);
        final long cardinality = row.getLong(4);

        return new DeletionVectorDescriptor(storageType, pathOrInlineDv, offset,
                sizeInBytes, cardinality);
    }

    // Markers to separate different kinds of DV storage.
    public static final String PATH_DV_MARKER = "p";
    public static final String INLINE_DV_MARKER = "i";
    public static final String UUID_DV_MARKER = "u";

    public static final StructType READ_SCHEMA = new StructType()
            .add("storageType", StringType.INSTANCE)
            .add("pathOrInlineDv", StringType.INSTANCE)
            .add("offset", IntegerType.INSTANCE)
            .add("sizeInBytes", IntegerType.INSTANCE)
            .add("cardinality", LongType.INSTANCE);

    ////////////////////////////////////////////////////////////////////////////////
    // Instance Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    /**
     * Indicates how the DV is stored.
     * Should be a single letter (see [[pathOrInlineDv]] below.)
     */
    private final String storageType;

    /**
     * Contains the actual data that allows accessing the DV.
     *
     * Three options are currently supported:
     * - `storageType="u"` format: `<random prefix - optional><base85 encoded uuid>`
     *                            The deletion vector is stored in a file with a path relative to
     *                            the data directory of this Delta Table, and the file name can be
     *                            reconstructed from the UUID.
     *                            The encoded UUID is always exactly 20 characters, so the random
     *                            prefix length can be determined any characters exceeding 20.
     * - `storageType="i"` format: `<base85 encoded bytes>`
     *                            The deletion vector is stored inline in the log.
     * - `storageType="p"` format: `<absolute path>`
     *                             The DV is stored in a file with an absolute path given by this
     *                             url.
     */
    private final String pathOrInlineDv;

    /**
     * Start of the data for this DV in number of bytes from the beginning of the file it is stored
     * in.
     *
     * Always None when storageType = "i".
     */
    private final int offset;

    /** Size of the serialized DV in bytes (raw data size, i.e. before base85 encoding). */
    private final int sizeInBytes;

    /** Number of rows the DV logically removes from the file. */
    private final long cardinality;

    public DeletionVectorDescriptor(
            String storageType,
            String pathOrInlineDv,
            int offset,
            int sizeInBytes,
            long cardinality) {
        this.storageType = storageType;
        this.pathOrInlineDv = pathOrInlineDv;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
        this.cardinality = cardinality;
    }

    public String getStorageType() { return storageType; }

    public String getPathOrInlineDv() { return pathOrInlineDv; }

    public int getOffset() { return offset; }

    public int getSizeInBytes() { return sizeInBytes; }

    public long getCardinality() { return cardinality; }

    @Override
    public String toString() {
        // todo
        return "";
    }

    // TODO: this seems round-about and WEIRD; decide between the two below approaches
    // can either do DeletionVectorDescriptorColumnarBatch (singleton) --> ColumnarBatchRow
    // or some sort of DeletionVectorDescriptorRow
    // if we extend and create DeletionVectorDescriptorRow we can add getters?
    public Row asRow() {
        return new PojoRow(
                this,
                READ_SCHEMA,
                ordinalToAccessor);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Static fields to create PojoRows
    ////////////////////////////////////////////////////////////////////////////////

    private static final Map<Integer, Function<DeletionVectorDescriptor, Object>> ordinalToAccessor = new HashMap<>();

    static {
        ordinalToAccessor.put(0, (a) -> a.getStorageType());
        ordinalToAccessor.put(1, (a) -> a.getPathOrInlineDv());
        ordinalToAccessor.put(2, (a) -> a.getOffset());
        ordinalToAccessor.put(3, (a) -> a.getSizeInBytes());
        ordinalToAccessor.put(4, (a) -> a.getCardinality());
    }
}