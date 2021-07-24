/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.runtime.batch.sql.agg

import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.junit.Test

class LeadLagITCase extends BatchTestBase{

  override def before(): Unit = {
    super.before()
    tEnv.executeSql(
      """
        |CREATE TEMPORARY VIEW T AS
        |SELECT * FROM ( VALUES
        |  ('Alice', 'alice@test.com', ARRAY [ 'alice@test.com' ], 'Test Ltd1'),
        |  ('Alice', 'alice@test.com', ARRAY [ 'alice@test.com' ], 'Test Ltd2'),
        |  ('Alice', 'alice@test.com', ARRAY [ 'alice@test.com', 'alice@test2.com' ], 'Test Ltd3'))
        |AS t ( name, email, aliases, company )
        |""".stripMargin)
  }

  @Test
  def testLeadLag(): Unit = {
    checkResult(
      """
        |SELECT
        |  name,
        |  company AS company1,
        |  LEAD(company, 1,'null') over (
        |    partition by name
        |)  AS company2
        |FROM T
        |""".stripMargin,
      Seq(
        row("Alice", "Test Ltd1", "Test Ltd2"),
        row("Alice", "Test Ltd2", "Test Ltd3"),
        row("Alice", "Test Ltd3", null)))

    checkResult(
      """
        |SELECT
        |  name,
        |  company AS company1,
        |  LAG(company, 1,'null') over (
        |    partition by name
        |)  AS company2
        |FROM T
        |""".stripMargin,
      Seq(
        row("Alice", "Test Ltd1", null),
        row("Alice", "Test Ltd2", "Test Ltd1"),
        row("Alice", "Test Ltd3", "Test Ltd2")))
  }
}
