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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

object ZOrderingCommand extends VacuumCommandImpl with Serializable {

  def zOrdering(
           spark: SparkSession,
           deltaLog: DeltaLog,
           column: String): Unit = {
    val path = deltaLog.dataPath
    val deltaHadoopConf = deltaLog.newDeltaHadoopConf()
    val fs = path.getFileSystem(deltaHadoopConf)
    val basePath = fs.makeQualified(path).toString

    spark
      .read
      .format("delta")
      .load(basePath)
      .repartition(deltaLog.snapshot.numOfFiles.toInt)
      .sortWithinPartitions(column)
      .write
      .format("delta")
      .mode("overwrite")
      .save(basePath)
  }
}
