package io.delta.kernel.client;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FileDataReadResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.parquet.ParquetFooter;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;
import io.delta.kernel.utils.Utils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class DefaultParquetHandler
    implements ParquetHandler
{
    private final Configuration hadoopConf;

    public DefaultParquetHandler(Configuration hadoopConf)
    {
        this.hadoopConf = hadoopConf;
    }

    @Override
    public CloseableIterator<FileReadContext> contextualizeFileReads(
            CloseableIterator<Row> fileIter,
            Expression predicate)
    {
        return new CloseableIterator<FileReadContext>() {
            @Override
            public void close()
                    throws IOException
            {
                fileIter.close();
            }

            @Override
            public boolean hasNext()
            {
                return fileIter.hasNext();
            }

            @Override
            public FileReadContext next()
            {
                // using unit means we call next() on an interator over and over
                Row row = fileIter.next();
                return () -> row;
            }
        };
    }

    @Override
    public CloseableIterator<FileDataReadResult> readParquetFiles(
            CloseableIterator<FileReadContext> fileIter,
            StructType physicalSchema) throws IOException
    {
        return new CloseableIterator<FileDataReadResult>() {
            private FileReadContext currentFile;
            private CloseableIterator<ColumnarBatch> currentFileReader;

            @Override
            public void close()
                    throws IOException
            {
                if (currentFileReader != null) {
                    currentFileReader.close();
                }

                fileIter.close();
                // TODO: implement safe close of multiple closeables.
            }

            @Override
            public boolean hasNext()
            {
                // There is no file in reading or the current file being read has no more data
                // initialize the next file reader or return false if there are no more files to
                // read.
                if (currentFileReader == null || !currentFileReader.hasNext()) {
                    if (fileIter.hasNext()) {
                        currentFile = fileIter.next();
                        FileStatus fileStatus = Utils.getFileStatus(currentFile.getScanFileRow());
                        ParquetBatchReader batchReader = new ParquetBatchReader(hadoopConf);
                        currentFileReader = batchReader.read(fileStatus.getPath(), physicalSchema);
                    } else {
                        return false;
                    }
                }

                return currentFileReader.hasNext();
            }

            @Override
            public FileDataReadResult next()
            {
                final ColumnarBatch data = currentFileReader.next();
                return new FileDataReadResult() {
                    @Override
                    public ColumnarBatch getData()
                    {
                        return data;
                    }

                    @Override
                    public Row getScanFileRow()
                    {
                        return currentFile.getScanFileRow();
                    }
                };
            }
        };
    }
}
