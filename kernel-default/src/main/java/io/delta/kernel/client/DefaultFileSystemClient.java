package io.delta.kernel.client;

import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;
import io.delta.storage.LocalLogStore;
import io.delta.storage.LogStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

public class DefaultFileSystemClient
    implements FileSystemClient
{
    private final Configuration hadoopConf;
    private final LogStore logStore;

    public DefaultFileSystemClient(Configuration hadoopConf)
    {
        this.hadoopConf = hadoopConf;
        this.logStore = new LocalLogStore(hadoopConf);
    }

    @Override
    public CloseableIterator<FileStatus> listFrom(String filePath)
            throws FileNotFoundException
    {
        return new CloseableIterator<FileStatus>() {
            private final Iterator<org.apache.hadoop.fs.FileStatus> iter;

            {
                try {
                    iter = logStore.listFrom(new Path(filePath), hadoopConf);
                } catch (IOException ex) {
                    throw new RuntimeException("Could not resolve the FileSystem", ex);
                }
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public FileStatus next() {
                final org.apache.hadoop.fs.FileStatus impl = iter.next();
                return FileStatus.of(
                        impl.getPath().toString(),
                        impl.getLen(),
                        impl.getModificationTime());
            }

            @Override
            public void close() throws IOException { }
        };
    }

    private ByteArrayInputStream getStream(String filePath, Integer offset, Integer size) {
        Path path = new Path(filePath);
        try {
            FileSystem fs = path.getFileSystem(hadoopConf);
            DataInputStream stream = null;
            try {
                stream = fs.open(path);
                stream.skipBytes(offset);
                byte[] buff = new byte[size];
                stream.readFully(buff);
                return new ByteArrayInputStream(buff);
            } catch (IOException ex) {
                // TODO: exception handling here (iterator.next())
                throw new RuntimeException(String.format(
                        "IOException reading from file %s at offset %s size %s",
                        filePath, offset, size), ex);
            } finally {
                if (stream != null) {
                    stream.close();
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException(String.format(
                    "Could not resolve the FileSystem for path %s", filePath), ex);
        }
    }

    @Override
    public CloseableIterator<ByteArrayInputStream> readFiles(
            CloseableIterator<Tuple2<String, Tuple2<Integer, Integer>>> iter) {
         return iter.map(elem -> getStream(elem._1, elem._2._1, elem._2._2));
    }
}
