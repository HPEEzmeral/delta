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

package org.apache.spark.sql.delta.util

import org.apache.commons.lang3.StringUtils

object ZOrderingUtils {
  private var isZOrdering: Boolean = false
  private var basePath: String = ""

  def getIsZOrdering: Boolean = {
    isZOrdering
  }

  def enableZOrdering(): Unit = {
    this.isZOrdering = true
  }

  def disableZOrdering(): Unit = {
    this.isZOrdering = false
  }

  def getBasePath: String = {
    basePath
  }

  def setBasePath(basePath: String): Unit = {
    this.basePath = StringUtils.substringAfter(basePath, ":").concat("/")
  }

}
