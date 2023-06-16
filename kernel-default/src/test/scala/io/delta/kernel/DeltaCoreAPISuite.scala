package io.delta.kernel

import java.util.Optional

import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.delta.kernel.client.{DefaultTableClient, TableClient}
import io.delta.kernel.data.{ColumnarBatch, ColumnVector, JsonRow, Row}
import io.delta.kernel.expressions.{Expression, Literal}
import io.delta.kernel.types.{ArrayType, BooleanType, IntegerType, LongType, MapType, StringType, StructType}
import io.delta.kernel.util.GoldenTableUtils
import io.delta.kernel.utils.{CloseableIterator, Utils}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class DeltaCoreAPISuite extends AnyFunSuite with GoldenTableUtils with DeltaKernelTestUtils {
  test("end-to-end usage: reading a table") {
    withGoldenTable("delta-table") { path =>
      val tableClient = DefaultTableClient.create(new Configuration())
      val table = Table.forPath(path)
      val snapshot = table.getLatestSnapshot(tableClient)

      // Contains both the data schema and partition schema
      val tableSchema = snapshot.getSchema(tableClient)

      // Go through the tableSchema and select the columns interested in reading
      val readSchema = new StructType().add("id", LongType.INSTANCE)
      val filter = Literal.TRUE

      val scanObject = scan(tableClient, snapshot, readSchema, filter)

      val fileIter = scanObject.getScanFiles(tableClient)
      val scanState = scanObject.getScanState(tableClient);

      // There should be just one element in the scan state
      val serializedScanState = convertColumnarBatchRowToJSON(scanState)

      val actualValueColumnValues = ArrayBuffer[Long]()
      while(fileIter.hasNext) {
        val fileColumnarBatch = fileIter.next()
        Seq.range(start = 0, end = fileColumnarBatch.getSize).foreach(rowId => {
          val serializedFileInfo = convertColumnarBatchRowToJSON(fileColumnarBatch, rowId)

          // START OF THE CODE THAT WILL BE EXECUTED ON THE EXECUTOR
          val dataBatches = Scan.readData(
            tableClient,
            scanState,
            // convertJSONToRow(serializedScanState, scanState.getSchema),
            Utils.singletonCloseableIterator(
              convertJSONToRow(serializedFileInfo, fileColumnarBatch.getSchema)),
            Optional.empty()
          )

          while(dataBatches.hasNext) {
            val batch = dataBatches.next()
            val valueColVector = batch.getData.getColumnVector(0)
            actualValueColumnValues.append(vectorToLongs(valueColVector): _*)
          }
          // END OF THE CODE THAT WILL BE EXECUTED ON THE EXECUTOR
        })
      }
      assert(actualValueColumnValues.toSet === Seq.range(start = 0, end = 20).toSet)
    }
  }

  test("end-to-end usage: reading a table with checkpoint") {
    withGoldenTable("basic-with-checkpoint") { path =>
      val tableClient = DefaultTableClient.create(new Configuration())
      val table = Table.forPath(path)
      val snapshot = table.getLatestSnapshot(tableClient)

      // Contains both the data schema and partition schema
      val tableSchema = snapshot.getSchema(tableClient)

      // Go through the tableSchema and select the columns interested in reading
      val readSchema = new StructType().add("id", LongType.INSTANCE)
      val filter = Literal.TRUE

      val scanObject = scan(tableClient, snapshot, readSchema, filter)

      val fileIter = scanObject.getScanFiles(tableClient)
      val scanState = scanObject.getScanState(tableClient);

      // There should be just one element in the scan state
      val serializedScanState = convertColumnarBatchRowToJSON(scanState)

      val actualValueColumnValues = ArrayBuffer[Long]()
      while(fileIter.hasNext) {
        val fileColumnarBatch = fileIter.next()
        Seq.range(start = 0, end = fileColumnarBatch.getSize).foreach(rowId => {
          val serializedFileInfo = convertColumnarBatchRowToJSON(fileColumnarBatch, rowId)

          // START OF THE CODE THAT WILL BE EXECUTED ON THE EXECUTOR
          val dataBatches = Scan.readData(
            tableClient,
            scanState,
            // convertJSONToRow(serializedScanState, scanState.getSchema),
            Utils.singletonCloseableIterator(
              convertJSONToRow(serializedFileInfo, fileColumnarBatch.getSchema)),
            Optional.empty()
          )

          while(dataBatches.hasNext) {
            val batch = dataBatches.next()
            val valueColVector = batch.getData.getColumnVector(0)
            actualValueColumnValues.append(vectorToLongs(valueColVector): _*)
          }
          // END OF THE CODE THAT WILL BE EXECUTED ON THE EXECUTOR
        })
      }
      assert(actualValueColumnValues.toSet === Seq.range(start = 0, end = 150).toSet)
    }
  }

  test("end-to-end usage: reading a table with dv") {
    withGoldenTable("basic-dv-no-checkpoint") { path =>
      val tableClient = DefaultTableClient.create(new Configuration())
      val table = Table.forPath(path)
      val snapshot = table.getLatestSnapshot(tableClient)

      // Go through the tableSchema and select the columns interested in reading
      val readSchema = new StructType().add("id", LongType.INSTANCE)
      val filter = Literal.TRUE

      val scanObject = scan(tableClient, snapshot, readSchema, filter)

      val fileIter = scanObject.getScanFiles(tableClient)
      val scanState = scanObject.getScanState(tableClient);

      val actualValueColumnValues = ArrayBuffer[Long]()
      while(fileIter.hasNext) {
        val fileColumnarBatch = fileIter.next()
          val dataBatches = Scan.readData(
            tableClient,
            scanState,
            fileColumnarBatch.getRows(),
            Optional.empty()
          )

          while (dataBatches.hasNext) {
            val batch = dataBatches.next()
            val selectionVector = batch.getSelectionVector()
            val valueColVector = batch.getData.getColumnVector(0)
            (0 to valueColVector.getSize()-1).foreach { i =>
              if (!selectionVector.isPresent || selectionVector.get.getBoolean(i)) {
                actualValueColumnValues.append(valueColVector.getLong(i))
              }
            }
          }
      }
      assert(actualValueColumnValues.toSet === Seq.range(start = 2, end = 10).toSet)
    }
  }

  test("reads DeletionVectorDescriptor from json files") {
    withGoldenTable("basic-dv-no-checkpoint") { path =>

      val readSchema = new StructType().add("id", LongType.INSTANCE)

      val tableClient = DefaultTableClient.create(new Configuration())
      val table = Table.forPath(path)
      val snapshot = table.getLatestSnapshot(tableClient)
      val scan = snapshot.getScanBuilder(tableClient)
        .withReadSchema(tableClient, readSchema)
        .build()

      val scanFilesIter = scan.getScanFiles(tableClient)
      val rows = getRows(scanFilesIter)
      val dvs = rows.filter(!_.isNullAt(5)).map(_.getRecord(5))

      // there should be 1 deletion vector
      assert(dvs.length == 1)

      val dv = dvs.head
      // storageType should be 'u'
      assert(dv.getString(0) == "u")
      // cardinality should be 2
      assert(dv.getLong(4) == 2)
    }
  }

  // TODO: test that log replay with DV works correctly. More DV tests.

  private def convertColumnarBatchRowToJSON(columnarBatch: ColumnarBatch, rowIndex: Int): String = {
    val rowObject = new java.util.HashMap[String, Object]()

    import scala.collection.JavaConverters._
    val schema = columnarBatch.getSchema
    schema.fields().asScala.zipWithIndex.foreach {
      case (field, index) =>
        val dataType = field.getDataType
        if (dataType.isInstanceOf[StructType]) {
          rowObject.put(field.getName, columnarBatch.getColumnVector(index).getStruct(rowIndex))
        } else if (dataType.isInstanceOf[ArrayType]) {
          rowObject.put(field.getName, columnarBatch.getColumnVector(index).getArray(rowIndex))
        } else if (dataType.isInstanceOf[MapType]) {
          rowObject.put(field.getName, columnarBatch.getColumnVector(index).getMap(rowIndex))
        } else if (dataType.isInstanceOf[IntegerType]) {
          rowObject.put(field.getName,
            new Integer(columnarBatch.getColumnVector(index).getInt(rowIndex)))
        } else if (dataType.isInstanceOf[LongType]) {
          rowObject.put(field.getName,
            new java.lang.Long(columnarBatch.getColumnVector(index).getLong(rowIndex)))
        } else if (dataType.isInstanceOf[BooleanType]) {
          rowObject.put(field.getName,
            new java.lang.Boolean(columnarBatch.getColumnVector(index).getBoolean(rowIndex)));
        } else if (dataType.isInstanceOf[StringType]) {
          rowObject.put(field.getName,
            columnarBatch.getColumnVector(index).getString(rowIndex))
        } else {
          throw new UnsupportedOperationException(field.getDataType.toString)
        }
    }
    new ObjectMapper().writeValueAsString(rowObject)
  }

  private def convertColumnarBatchRowToJSON(row: Row): String = {
    val rowObject = new java.util.HashMap[String, Object]()

    import scala.collection.JavaConverters._
    val schema = row.getSchema
    schema.fields().asScala.zipWithIndex.foreach {
      case (field, index) =>
        val dataType = field.getDataType
        if (dataType.isInstanceOf[StructType]) {
          rowObject.put(field.getName, row.getRecord(index))
        } else if (dataType.isInstanceOf[ArrayType]) {
          rowObject.put(field.getName, row.getList(index))
        } else if (dataType.isInstanceOf[MapType]) {
          rowObject.put(field.getName, row.getMap(index))
        } else if (dataType.isInstanceOf[IntegerType]) {
          rowObject.put(field.getName, new Integer(row.getInt(index)))
        } else if (dataType.isInstanceOf[LongType]) {
          rowObject.put(field.getName, new java.lang.Long(row.getLong(index)))
        } else if (dataType.isInstanceOf[BooleanType]) {
          rowObject.put(field.getName, new java.lang.Boolean(row.getBoolean(index)))
//        } else if (dataType.isInstanceOf[DoubleType]) {
//          rowObject.put(field.getName,
//            new java.lang.Double(columnarBatch.getColumnVector(index).getDouble(0)))
//        } else if (dataType.isInstanceOf[FloatType]) {
//          rowObject.put(field.getName,
//            new java.lang.Float(columnarBatch.getColumnVector(index).getFloat(0)))
        } else if (dataType.isInstanceOf[StringType]) {
          rowObject.put(field.getName, row.getString(index))
        } else {
          throw new UnsupportedOperationException(field.getDataType.toString)
        }
    }

    new ObjectMapper().writeValueAsString(rowObject)
  }

  private def convertJSONToRow(json: String, readSchema: StructType): Row = {
    try {
      val jsonNode = new ObjectMapper().readTree(json)
      new JsonRow(jsonNode.asInstanceOf[ObjectNode], readSchema)
    } catch {
      case ex: JsonProcessingException =>
        throw new RuntimeException(String.format("Could not parse JSON: %s", json), ex)
    }
  }

  private def scan(
    tableClient: TableClient,
    snapshot: Snapshot,
    readSchema: StructType,
    filter: Expression = null): Scan = {
    var builder =
      snapshot.getScanBuilder(tableClient).withReadSchema(tableClient, readSchema)
    if (filter != null) {
      builder = builder.withFilter(tableClient, filter)
    }
    builder.build()
  }

  private def vectorToInts(intColumnVector: ColumnVector): Seq[Int] = {
    Seq.range(start = 0, end = intColumnVector.getSize)
      .map(rowId => intColumnVector.getInt(rowId))
      .toSeq
  }

  private def vectorToLongs(longColumnVector: ColumnVector): Seq[Long] = {
    Seq.range(start = 0, end = longColumnVector.getSize)
      .map(rowId => longColumnVector.getLong(rowId))
      .toSeq
  }
}
