package io.delta.kernel.internal.actions;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;

public class SingleAction {

    ////////////////////////////////////////////////////////////////////////////////
    // Static Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    public static SingleAction fromRow(Row row, TableClient tableClient) {
        final SetTransaction txn = SetTransaction.fromRow(row.getRecord(0));
        if (txn != null) {
            return new SingleAction(txn, null, null, null, null, null, null);
        }

        final AddFile add = AddFile.fromRow(row.getRecord(1));
        if (add != null) {
            return new SingleAction(null, add, null, null, null, null, null);
        }

        final RemoveFile remove = RemoveFile.fromRow(row.getRecord(2));
        if (remove != null) {
            return new SingleAction(null, null, remove, null, null, null, null);
        }

        final Metadata metadata = Metadata.fromRow(row.getRecord(3), tableClient);
        if (metadata != null) {
            return new SingleAction(null, null, null, metadata, null, null, null);
        }

        final Protocol protocol = Protocol.fromRow(row.getRecord(4));
        if (protocol != null) {
            return new SingleAction(null, null, null, null, protocol, null, null);
        }

//        final AddCDCFile cdc = AddCDCFile.fromRow(row.getRecord(5));
//        if (cdc != null) {
//            return new SingleAction(null, null, null, null, null, cdc, null);
//        }

        final CommitInfo commitInfo = CommitInfo.fromRow(row.getRecord(5));
        if (commitInfo != null) {
            return new SingleAction(null, null, null, null, null, null, commitInfo);
        }

        throw new IllegalStateException("SingleAction row contained no non-null actions");
    }

    public static StructType READ_SCHEMA = new StructType()
        .add("txn", SetTransaction.READ_SCHEMA)
        .add("add", AddFile.READ_SCHEMA)
        .add("remove", RemoveFile.READ_SCHEMA)
        .add("metaData", Metadata.READ_SCHEMA)
        .add("protocol", Protocol.READ_SCHEMA)
        // TODO: current Parquet reader doesn't handle missing columns in the files
        // TODO: ^^ is a problem for checkpoints without DVs!!!
        // .add("cdc", AddCDCFile.READ_SCHEMA)
        .add("commitInfo", CommitInfo.READ_SCHEMA);

    ////////////////////////////////////////////////////////////////////////////////
    // Instance Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    private final SetTransaction txn;
    private final AddFile add;
    private final RemoveFile remove;
    private final Metadata metadata;
    private final Protocol protocol;
    // private final AddCDCFile cdc;
    private final CommitInfo commitInfo;

    private SingleAction(
            SetTransaction txn,
            AddFile add,
            RemoveFile remove,
            Metadata metadata,
            Protocol protocol,
            AddCDCFile cdc,
            CommitInfo commitInfo) {
        this.txn = txn;
        this.add = add;
        this.remove = remove;
        this.metadata = metadata;
        this.protocol = protocol;
        // this.cdc = cdc;
        this.commitInfo = commitInfo;
    }

    public Action unwrap() {
        if (txn != null) return txn;
        if (add != null) return add;
        if (remove != null) return remove;
        if (metadata != null) return metadata;
        if (protocol != null) return protocol;
        // if (cdc != null) return cdc;
        if (commitInfo != null) return commitInfo;

        throw new IllegalStateException("SingleAction row contained no non-null actions");
    }
}
