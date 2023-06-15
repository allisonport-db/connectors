package io.delta.kernel.internal.actions;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

import io.delta.kernel.data.Row;
import io.delta.kernel.fs.Path;
import io.delta.kernel.internal.lang.Lazy;

public abstract class FileAction implements Action {
    protected final String path;
    protected final boolean dataChange;
    private final Lazy<URI> pathAsUri;
    private final Lazy<Path> pathAsPath;
    protected final DeletionVectorDescriptor deletionVector;

    public FileAction(String path, boolean dataChange, DeletionVectorDescriptor deletionVector) {
        this.path = path;
        this.dataChange = dataChange;
        this.deletionVector = deletionVector;

        this.pathAsUri = new Lazy<>(() -> {
            try {
                return new URI(path);
            } catch (URISyntaxException ex) {
                throw new RuntimeException(ex);
            }
        });

        this.pathAsPath = new Lazy<>(() -> new Path(path));
    }

    public String getPath() {
        return path;
    }

    public boolean isDataChange() {
        return dataChange;
    }

    public DeletionVectorDescriptor getDeletionVector() { return deletionVector; }

    public URI toURI() {
        return pathAsUri.get();
    }

    public Path toPath() {
        return pathAsPath.get();
    }

    public abstract FileAction copyWithDataChange(boolean dataChange);

    public Optional<String> getDeletionVectorUniqueId() {
        if (deletionVector == null) {
            return Optional.empty();
        } else {
            return Optional.of(deletionVector.getUniqueId());
        }
    }

    public Row getDeletionVectorAsRow() {
        if (deletionVector == null) {
            return null;
        } else {
            return deletionVector.asRow();
        }
    }

}
