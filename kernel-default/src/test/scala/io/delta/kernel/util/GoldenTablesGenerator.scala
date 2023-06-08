/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.kernel.util

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession

/**
 * GENERATE_GOLDEN_TABLES=1 build/sbt 'kernelDefault/testOnly *GoldenTablesGenerator'
 * or
 * GENERATE_GOLDEN_TABLES=1 build/sbt 'kernelDefault/testOnly *GoldenTablesGenerator -- -z "<table-name>"'
 */
class GoldenTablesGenerator extends QueryTest with SharedSparkSession {

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

  private val shouldGenerateGoldenTables = sys.env.contains("GENERATE_GOLDEN_TABLES")

  private lazy val resourcesDirectory = {
    val dir = new File("src/test/resources").getCanonicalFile
    require(dir.exists())
    dir
  }

  private def generate(tableName: String)(generator: String => Unit): Unit = {
    if (shouldGenerateGoldenTables) {
      test(tableName) {
        val dir = new File(resourcesDirectory, tableName)
        JavaUtils.deleteRecursively(dir)

        generator(dir.getCanonicalPath)
      }
    }
  }

  generate("basic-no-checkpoint") { path =>
    for (i <- 0 to 8) {
      val low = i * 10
      val high = (i + 1) * 10
      spark.range(low, high).write.format("delta").mode("append").save(path)
    }
  }

  generate("basic-with-checkpoint") { path =>
    for (i <- 0 to 14) {
      val low = i * 10
      val high = (i + 1) * 10
      spark.range(low, high).write.format("delta").mode("append").save(path)
    }
  }

  generate("basic-partitioned-no-checkpoint") { path =>
    for (i <- 0 to 8) {
      val low = i * 10
      val high = (i + 1) * 10
      spark.range(low, high)
        .withColumn("part_a", col("id") % 2)
        .withColumn("part_b", col("id") % 5)
        .write
        .format("delta")
        .partitionBy("part_a", "part_b")
        .mode("append").save(path)
    }
  }

  generate("basic-dv-no-checkpoint") { path =>
    withSQLConf(("spark.databricks.delta.properties.defaults.enableDeletionVectors", "true")) {
      spark.range(10)
        .write
        .format("delta")
        .save(path)
      spark.sql(
        s"""
          |DELETE FROM delta.`$path` WHERE id < 2
          |""".stripMargin)
    }
  }

  generate("parquet-basic-row-indexes") { path =>
    // write three files such that the row index should = id % 10
    spark.range(0, 10)
      .coalesce(1)
      .sortWithinPartitions("id")
      .write
      .parquet(path)
    spark.range(10, 20)
      .coalesce(1)
      .sortWithinPartitions("id")
      .write
      .mode("append")
      .parquet(path)
    spark.range(20, 30)
      .coalesce(1)
      .sortWithinPartitions("id")
      .write
      .mode("append")
      .parquet(path)
  }
}
