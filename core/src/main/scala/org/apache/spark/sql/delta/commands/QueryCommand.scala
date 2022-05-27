/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.commands

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

object QueryCommand extends VacuumCommandImpl with Serializable {

  def sparkQuery(
                  spark: SparkSession,
                  deltaLog: DeltaLog,
                  query: String): Unit = {
    val (basePath: String, logPath: String) = deltaPaths(deltaLog)

    val paths = readStats(spark, logPath).map { f =>
      val stats = spark.read.json(spark.sparkContext.parallelize(Seq(f._2)))
      val minMaxTable = stats.select("minValues.value").union(stats.select("maxValues.value"))
        .withColumnRenamed("value", stats.select("minValues.key")
          .collect().head.toString().replaceAll("[\\[\\]]", ""))
        .where(query)

      if (minMaxTable.count() > 0) basePath.concat("/").concat(f._1) else StringUtils.EMPTY
    }.filter(_.nonEmpty)

    spark
      .read
      .parquet(paths: _*)
      .where(query).show()
  }

  def sqlQuery(
                spark: SparkSession,
                deltaLog: DeltaLog,
                query: String): Unit = {
    val (basePath: String, logPath: String) = deltaPaths(deltaLog)

    val tableName = StringUtils.substringAfterLast(basePath, "/")
    val validTableName = if (tableName.contains("-")) "`" + tableName + "`" else tableName

    val paths = readStats(spark, logPath).map { f =>
      val stats = spark.read.json(spark.sparkContext.parallelize(Seq(f._2)))
      val minMaxTable = stats.select("minValues.value").union(stats.select("maxValues.value"))
        .withColumnRenamed("value", stats.select("minValues.key")
          .collect().head.toString().replaceAll("[\\[\\]]", ""))

      minMaxTable.createOrReplaceTempView(validTableName)
      val result = minMaxTable.sqlContext.sql(query)

      if (result.count() > 0) basePath.concat("/").concat(f._1) else StringUtils.EMPTY
    }.filter(_.nonEmpty)

    val parquetsDf = spark
      .read
      .parquet(paths: _*)

    parquetsDf.createOrReplaceTempView(validTableName)

    parquetsDf.sqlContext.sql(query).show()
  }

  private def deltaPaths(deltaLog: DeltaLog) = {
    val path = deltaLog.dataPath
    val deltaHadoopConf = deltaLog.newDeltaHadoopConf()
    val fs = path.getFileSystem(deltaHadoopConf)
    val basePath = fs.makeQualified(path).toString
    val logPath = fs.listStatus(deltaLog.logPath).last.getPath.toString
    (basePath, logPath)
  }

  private def readStats(spark: SparkSession, logPath: String) = {
    spark.read.json(logPath).select("add.path", "add.stats")
      .collect()
      .map(_.toString().replaceAll("[\\[\\]]", ""))
      .filter(!_.contains("null"))
      .map(x => (StringUtils.substringBefore(x, ","), StringUtils.substringAfter(x, ",")))
  }
}
