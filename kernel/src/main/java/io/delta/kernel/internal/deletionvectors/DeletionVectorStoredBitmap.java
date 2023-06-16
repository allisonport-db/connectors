package io.delta.kernel.internal.deletionvectors;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.zip.CRC32;

import io.delta.kernel.client.FileSystemClient;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;
import io.delta.kernel.utils.Utils;

/**
 * Bitmap for a Deletion Vector, implemented as a thin wrapper around a Deletion Vector
 * Descriptor. The bitmap can be empty, inline or on-disk. In case of on-disk deletion
 * vectors, `tableDataPath` must be set to the data path of the Delta table, which is where
 * deletion vectors are stored.
 */
public class DeletionVectorStoredBitmap {

    private final DeletionVectorDescriptor dvDescriptor;
    private final Optional<String> tableDataPath;

    public DeletionVectorStoredBitmap(
            DeletionVectorDescriptor dvDescriptor,
            Optional<String> tableDataPath) {
        if (!(tableDataPath.isPresent() || !dvDescriptor.isOnDisk())) {
            throw new IllegalArgumentException(
                    "Table path is required for on-disk deletion vectors");
        }
        this.dvDescriptor = dvDescriptor;
        this.tableDataPath = tableDataPath;
    }

    // TODO: for now we request 1 stream at a time
    public RoaringBitmapArray load(FileSystemClient fileSystemClient) throws IOException {
        if (dvDescriptor.getCardinality() == 0) { // isEmpty
            return new RoaringBitmapArray();
        } else if (dvDescriptor.isInline()) {
            return RoaringBitmapArray.readFrom(dvDescriptor.inlineData());
        } else { // isOnDisk
            String onDiskPath = dvDescriptor.getAbsolutePath(tableDataPath.get());

            // TODO: this type is TBD
            Tuple2<String, Tuple2<Integer, Integer>> dvToRead =
                    new Tuple2(
                            onDiskPath, // filePath
                            new Tuple2(
                                    Optional.of(dvDescriptor.getOffset()).orElse(0), // offset
                                    // we pad 4 bytes in the front for the size
                                    // and 4 bytes at the end for CRC-32 checksum
                                    dvDescriptor.getSizeInBytes() + 8 // size
                            )
                    );

            CloseableIterator<ByteArrayInputStream> streamIter = fileSystemClient.readFiles(
                    Utils.singletonCloseableIterator(dvToRead));
            if (streamIter.hasNext()) {
                ByteArrayInputStream stream = streamIter.next();
                streamIter.close();
                return loadFromStream(stream);
            } else {
                // should not happen
                throw new IllegalStateException("readFiles returned an empty iterator");
            }
        }
    }

    /**
     * Read a serialized deletion vector from a data stream.
     */
    private RoaringBitmapArray loadFromStream(ByteArrayInputStream stream) throws IOException {
        DataInputStream dataStream = new DataInputStream(stream);
        try {
            int sizeAccordingToFile = dataStream.readInt();
            if (dvDescriptor.getSizeInBytes() != sizeAccordingToFile) {
                throw new RuntimeException("DV size mismatch");
            }

            byte[] buffer = new byte[sizeAccordingToFile];
            dataStream.readFully(buffer);

            int expectedChecksum = dataStream.readInt();
            int actualChecksum = calculateChecksum(buffer);
            if (expectedChecksum != actualChecksum) {
                throw new RuntimeException("DV checksum mismatch");
            }
            return RoaringBitmapArray.readFrom(buffer);
        } finally {
            stream.close();
            dataStream.close();
        }
    }

    /**
     * Calculate checksum of a serialized deletion vector. We are using CRC32 which has 4bytes size,
     * but CRC32 implementation conforms to Java Checksum interface which requires a long. However,
     * the high-order bytes are zero, so here is safe to cast to Int. This will result in negative
     * checksums, but this is not a problem because we only care about equality.
     */
    private int calculateChecksum(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }
}
