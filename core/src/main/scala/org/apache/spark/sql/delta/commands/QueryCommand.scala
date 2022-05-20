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

  def query(
             spark: SparkSession,
             deltaLog: DeltaLog,
             sqlText: String): Unit = {
    val path = deltaLog.dataPath
    val deltaHadoopConf = deltaLog.newDeltaHadoopConf()
    val fs = path.getFileSystem(deltaHadoopConf)
    val basePath = fs.makeQualified(path).toString
    val logPath = fs.listStatus(deltaLog.logPath).last.getPath.toString

    val paths = spark.read.json(logPath).select("add.path", "add.stats")
      .collect()
      .map(_.toString().replaceAll("[\\[\\]]", ""))
      .filter(!_.contains("null"))
      .map(x => (StringUtils.substringBefore(x, ","), StringUtils.substringAfter(x, ",")))
      .map { f =>
        val x = spark.read.json(spark.sparkContext.parallelize(Seq(f._2)))
        val y = x.select("minValues.value").union(x.select("maxValues.value"))
          .withColumnRenamed("value", x.select("minValues.key")
            .collect().head.toString().replaceAll("[\\[\\]]", ""))
          .where(sqlText)

        if (y.count() > 0) basePath.concat("/").concat(f._1) else StringUtils.EMPTY
      }.filter(_.nonEmpty)

    spark
      .read
      .parquet(paths: _*)
      .where(sqlText).show()
  }
}
