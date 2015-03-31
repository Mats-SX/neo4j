/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import scala.util.matching.Regex

class EagerizationAcceptanceTest extends ExecutionEngineFunSuite {
  val EagerRegEx: Regex = "Eager(?!A)".r

  test("should not introduce eagerness for MATCH nodes and CREATE relationships") {
    val query = "MATCH a, b CREATE (a)-[:KNOWS]->(b)"

    assertNumberOfEagerness(query, 0)
  }

  test("should introduce eagerness when doing first matching and then creating nodes") {
    val query = "MATCH a CREATE (b)"

    assertNumberOfEagerness(query, 1)
  }

  test("should not introduce eagerness for MATCH nodes and CREATE UNIQUE relationships") {
    val query = "MATCH a, b CREATE UNIQUE (a)-[r:KNOWS]->(b)"

    assertNumberOfEagerness(query, 0)
  }

  test("should not introduce eagerness for MATCH nodes and MERGE relationships") {
    val query = "MATCH a, b MERGE (a)-[r:KNOWS]->(b)"

    assertNumberOfEagerness(query, 0)
  }

  test("should not add eagerness when not writing to nodes") {
    val query = "MATCH a, b CREATE (a)-[r:KNOWS]->(b) SET r = { key: 42 }"

    assertNumberOfEagerness(query, 0)
  }

  test("should not introduce eagerness when the ON MATCH includes writing to a non-matched property") {
    val query = "MATCH (a:Foo), (b:Bar) MERGE (a)-[r:KNOWS]->(b) ON MATCH SET a.prop = 42"

    assertNumberOfEagerness(query, 0)
  }

  test("should introduce eagerness when the ON MATCH includes writing to a matched label") {
    val query = "MATCH (a:Foo), (b:Bar) MERGE (a)-[r:KNOWS]->(b) ON MATCH SET b:Foo"

    assertNumberOfEagerness(query, 1)
  }

  test("should understand symbols introduced by FOREACH") {
    val query =
      """MATCH (a:Label)
        |WITH collect(a) as nodes
        |MATCH (b:Label2)
        |FOREACH(n in nodes |
        |  CREATE UNIQUE (n)-[:SELF]->(b))""".stripMargin

    assertNumberOfEagerness(query, 0)
  }

  test("LOAD CSV FROM 'file:///something' AS line MERGE (b:B {p:line[0]}) RETURN b") {
    val query = "LOAD CSV FROM 'file:///something' AS line MERGE (b:B {p:line[0]}) RETURN b"

    assertNumberOfEagerness(query, 0)
  }

  test("MATCH (a:Person),(m:Movie) OPTIONAL MATCH (a)-[r1]-(), (m)-[r2]-() DELETE a,r1,m,r2") {
    val query = "MATCH (a:Person),(m:Movie) OPTIONAL MATCH (a)-[r1]-(), (m)-[r2]-() DELETE a,r1,m,r2"

    assertNumberOfEagerness(query, 1)
  }

  test("MATCH (a:Person),(m:Movie) CREATE (a)-[:T]->(m) WITH a OPTIONAL MATCH (a) RETURN *") {
    val query = "MATCH (a:Person),(m:Movie) CREATE (a)-[:T]->(m) WITH a OPTIONAL MATCH (a) RETURN *"

    assertNumberOfEagerness(query, 0)
  }

  test("should not add eagerness when reading and merging nodes and relationships when matching different label") {
    val query = "MATCH (a:A) MERGE (a)-[:BAR]->(b:B) WITH a MATCH (a) WHERE (a)-[:FOO]->() RETURN a"

    assertNumberOfEagerness(query, 0)
  }

  test("should add eagerness when reading and merging nodes and relationships on matching same label") {
    val query = "MATCH (a:A) MERGE (a)-[:BAR]->(b:A) WITH a MATCH (a) WHERE (a)-[:FOO]->() RETURN a"

    assertNumberOfEagerness(query, 1)
  }

  test("should not add eagerness when reading nodes and merging relationships") {
    val query = "MATCH (a:A), (b:B) MERGE (a)-[:BAR]->(b) WITH a MATCH (a) WHERE (a)-[:FOO]->() RETURN a"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property and writing different property should not be eager") {
    val query = "MATCH (n:Node {prop:5}) SET n.value = 10"

    assertNumberOfEagerness(query, 0)
  }

  test("matching label and writing different label should not be eager") {
    val query = "MATCH (n:Node) SET n:Lol"

    assertNumberOfEagerness(query, 0)
  }

  test("matching label and writing same label should be eager") {
    val query = "MATCH (n:Lol) SET n:Lol"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property and writing label should not be eager") {
    val query = "MATCH (n {name : 'thing'}) SET n:Lol"

    assertNumberOfEagerness(query, 0)
  }

  test("matching label and writing property should not be eager") {
    val query = "MATCH (n:Lol) SET n.name = 'thing'"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property and writing property should be eager") {
    val query = "MATCH (n:Node {prop:5}) SET n.prop = 10"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property via index and writing same property should be eager"){
    execute("CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE")
    execute("CREATE (b:Book {isbn : '123'})")

    val query = "MATCH (b :Book {isbn : '123'}) SET b.isbn = '456'"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using AND and writing to same property should be eager") {
    val query = "MATCH n WHERE n.prop1 = 10 AND n.prop2 = 10 SET n.prop1 = 5"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using AND and writing to different property should not be eager") {
    val query = "MATCH n WHERE n.prop1 = 10 AND n.prop2 = 10 SET n.prop3 = 5"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using LARGER THAN and writing to same property should be eager") {
    val query = "MATCH n WHERE n.prop1 > 10 SET n.prop1 = 5"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using LARGER THAN and writing to different property should not be eager") {
    val query = "MATCH n WHERE n.prop1 > 10 SET n.prop2 = 5"

    assertNumberOfEagerness(query, 0)
  }

  private def assertNumberOfEagerness(query: String, expectedEagerCount: Int) {
    val q = if (query.contains("EXPLAIN")) query else "EXPLAIN " + query
    val result = execute(q)
    val plan = result.executionPlanDescription().toString
    result.close()
    val length = EagerRegEx.findAllIn(plan).length / 2
    assert(length == expectedEagerCount, plan)
  }
}
