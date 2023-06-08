package io.delta.kernel

import java.io.File

import io.delta.kernel.client.ParquetBatchReader
import io.delta.kernel.types.{LongType, StructField, StructType}
import io.delta.kernel.util.GoldenTableUtils
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class ParquetBatchReaderSuite extends AnyFunSuite with GoldenTableUtils with DeltaKernelTestUtils {

  test("don't request row-indexes") {
    withGoldenTable("parquet-basic-row-indexes") { path =>
      val dir = new File(path)
      val parquetFiles = dir.listFiles()
        .filter { file =>
          file.getName().endsWith(".parquet")
        }

      val readSchema = new StructType()
        .add("id", LongType.INSTANCE)
      val parquetBatchReader = new ParquetBatchReader(new Configuration())

      // there should be three files [0, 10), [10, 20), [20, 30)
      parquetFiles.flatMap { file =>
        val iter = parquetBatchReader.read(file.getAbsolutePath, readSchema)
        val rows = getRows(iter)
        rows.map(_.getLong(0))
      }.toSet == Set(Range(0, 30))
    }
  }

  test("request row indexes") {
    withGoldenTable("parquet-basic-row-indexes") { path =>
      val dir = new File(path)
      val parquetFiles = dir.listFiles()
        .filter { file =>
          file.getName().endsWith(".parquet")
        }

      val readSchema = new StructType()
        .add("id", LongType.INSTANCE)
        .add(StructField.ROW_INDEX_COLUMN)
      val parquetBatchReader = new ParquetBatchReader(new Configuration())

      parquetFiles.foreach { file =>
        val iter = parquetBatchReader.read(file.getAbsolutePath, readSchema)
        val rows = getRows(iter)
        // row index should = id % 10
        rows.foreach { row =>
          assert(row.getLong(0) % 10 == row.getLong(1))
        }
      }
    }
  }
}
