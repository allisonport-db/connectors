package io.delta.kernel

import io.delta.kernel.data.{ColumnarBatch, Row}
import io.delta.kernel.utils.CloseableIterator

trait DeltaKernelTestUtils {

  def getRows(batchIter: CloseableIterator[ColumnarBatch]): Seq[Row] = {
    var result = Seq.empty[Row]
    try {
      while (batchIter.hasNext) {
        val batch = batchIter.next()
        val rowIter = batch.getRows
        try {
          while (rowIter.hasNext) {
            result = result :+ rowIter.next
          }
        } finally {
          rowIter.close()
        }
      }
    } finally {
      batchIter.close()
    }
    result
  }
}
