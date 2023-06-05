package io.delta.kernel.internal.replay;

import java.io.IOException;
import java.util.stream.Stream;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.fs.Path;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.actions.Action;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.lang.CloseableIterable;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.utils.Tuple2;
import io.delta.kernel.utils.CloseableIterator;

public class LogReplay {

    private final Path logPath;
    private final Path dataPath;
    private final LogSegment logSegment;
    private final CloseableIterable<Tuple2<Action, Boolean>> reverseActionsIterable;
    private final Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata;

    public LogReplay(
            Path logPath,
            Path dataPath,
            TableClient tableHelper,
            LogSegment logSegment) {
        this.logPath = logPath;
        this.dataPath = dataPath;
        this.logSegment = logSegment;

        final Stream<FileStatus> allFiles = Stream.concat(
                logSegment.checkpoints.stream(),
                logSegment.deltas.stream());
        assertLogFilesBelongToTable(logPath, allFiles);

        this.reverseActionsIterable = new ReverseFilesToActionsIterable(
                tableHelper,
                allFiles);
        this.protocolAndMetadata = new Lazy<>(this::loadTableProtocolAndMetadata);
    }

    /////////////////
    // Public APIs //
    /////////////////

    public Lazy<Tuple2<Protocol, Metadata>> lazyLoadProtocolAndMetadata() {
        return this.protocolAndMetadata;
    }

    public CloseableIterator<AddFile> getAddFiles() {
        final CloseableIterator<Tuple2<Action, Boolean>> reverseActionsIter = reverseActionsIterable.iterator();
        return new ReverseActionsToAddFilesIterator(dataPath, reverseActionsIter);
    }

    /////////////////////
    // Private Helpers //
    /////////////////////

    private Tuple2<Protocol, Metadata> loadTableProtocolAndMetadata() {
        Protocol protocol = null;
        Metadata metadata = null;

        try (final CloseableIterator<Tuple2<Action, Boolean>> reverseIter = reverseActionsIterable.iterator()) {
            while (reverseIter.hasNext()) {
                final Action action = reverseIter.next()._1;

                if (action instanceof Protocol && protocol == null) {
                    // We only need the latest protocol
                    protocol = (Protocol) action;

                    if (metadata != null) {
                        // Stop since we have found the latest Protocol and Metadata.
                        return new Tuple2<>(protocol, metadata);
                    }
                } else if (action instanceof Metadata && metadata == null) {
                    // We only need the latest Metadata
                    metadata = (Metadata) action;

                    if (protocol != null) {
                        // Stop since we have found the latest Protocol and Metadata.
                        return new Tuple2<>(protocol, metadata);
                    }
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException("Could not close iterator", ex);
        }

        if (protocol == null) {
            throw new IllegalStateException(
                String.format("No protocol found at version %s", logSegment.version)
            );
        }

        throw new IllegalStateException(
            String.format("No metadata found at version %s", logSegment.version)
        );
    }

    /**
     * Verifies that a set of delta or checkpoint files to be read actually belongs to this table.
     */
    private void assertLogFilesBelongToTable(Path logPath, Stream<FileStatus> allFiles) {
        // TODO:
    }
}
