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

package org.apache.spark.sql.delta.files

import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader

object ParquetReader {
  private var columnName: String = ""

  def setColumnName(columnName: String): Unit = {
    this.columnName = columnName
  }

  def getColumnName: String = {
    columnName
  }

  def readParquet(path: Path): List[String] = {
    val reader = AvroParquetReader.builder[GenericRecord](path).build()
    val parquetIterator = Iterator.continually(reader.read).takeWhile(_ != null)
    val recordList = parquetIterator.toList
    recordList.map(record => record.get(columnName).toString)
  }
}
