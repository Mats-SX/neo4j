/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}

class KeysAcceptanceTest extends ExecutionEngineFunSuite  with QueryStatisticsTestSupport {

  test("fuse projections") {
    val query = """MATCH (u:user { id: { p01 } }), (x:file { id: { p02 } })
                  |    WITH u, x
                  |
                  |
                  |    MATCH (p:project {id: x.projectid})
                  |    WITH u, p, x
                  |
                  |    MATCH
                  |      (u)-[:CONTACT]->(c:contact {teamid: p.teamid})-[:CONTACT]->(cr:role {teamid: p.teamid})
                  |    WITH p, x, c, cr,
                  |      [r IN (c)-[:CONTACT]->(:projectrole {projectid: p.id}) | LAST(NODES(r))] AS prs
                  |    WITH p, x, c, cr, EXTRACT(r IN prs | r.name) AS prs
                  |    WITH x, c, {
                  |      accessedby: c.id,
                  |      accessedteamrole: cr.name,
                  |      accessedrole:
                  |        CASE WHEN cr.name IN ["Owners", "Admins"] THEN cr.name
                  |             WHEN "ProjectAdmins" IN prs THEN "ProjectAdmins"
                  |             WHEN "ProjectClients" IN prs THEN "ProjectClients"
                  |             WHEN "ProjectContacts" IN prs THEN "ProjectContacts"
                  |             ELSE "None"
                  |        END
                  |    } AS r
                  |
                  |
                  |    WITH x AS f, r
                  |
                  |
                  |    MATCH (ha:activity)<-[:HISTORY]-(f)-[:CREATED]->(ca:activity)
                  |    WITH f, {
                  |      accessedrole: r.accessedrole, accessedby: r.accessedby,
                  |      createdat: ca.createdat, createdby: {id: ca.contactid, object: "contact"},
                  |      updatedat: ha.createdat, updatedby: {id: ha.contactid, object: "contact"}
                  |      } AS r
                  |
                  |    MATCH (f)-->(x)
                  |    WITH f, COLLECT(x) AS xs, r
                  |    WITH f, {
                  |      accessedrole: r.accessedrole, accessedby: r.accessedby,
                  |      createdat: r.createdat, createdby: r.createdby,
                  |      updatedat: r.updatedat, updatedby: r.updatedby,
                  |      tags: [x IN xs WHERE x:tag | {
                  |        id: x.id, object: "tag", name: x.name
                  |        }]
                  |      } AS r
                  |
                  |    MATCH (x)-[:FILE]->(f)
                  |    WITH f, COLLECT(x) AS xs, r
                  |    RETURN COLLECT({
                  |      id: f.id, object: f.object, projectid: f.projectid,
                  |      isinternal: f.isinternal,
                  |      no: f.no, name: f.name, description: f.description, url: f.url,
                  |      mime: f.mime, mimetype: f.mimetype, thumbnail: f.thumbnail, size: f.size,
                  |      accessedrole: r.accessedrole, accessedby: r.accessedby,
                  |      createdat: r.createdat, createdby: r.createdby,
                  |      updatedat: r.updatedat, updatedby: r.updatedby,
                  |      tags: r.tags,
                  |      related: [x IN xs WHERE exists(x.object) | {
                  |        id: x.id, object: x.object
                  |        }]
                  |      }) AS r""".stripMargin

    val result = graph.execute(s"EXPLAIN $query")

    println(result.getExecutionPlanDescription)
  }
  test("fuse projections small") {
    val query = """MATCH (u), (x)
                  |    MATCH (p)
                  |    MATCH
                  |      (u)-->(c)-->(cr)
                  |    WITH u, x
                  |    WITH u, p, x
                  |    WITH p, x, c, cr, [r IN (c)-[:CONTACT]->(:projectrole {projectid: p.id}) | LAST(NODES(r))] AS prs
                  |    WITH p, x, c, cr, EXTRACT(r IN prs | r.name) AS prs
                  |    WITH x, c, {
                  |      accessedby: c.id,
                  |      accessedteamrole: cr.name,
                  |      accessedrole:
                  |        CASE WHEN cr.name IN ["Owners", "Admins"] THEN cr.name
                  |             WHEN "ProjectAdmins" IN prs THEN "ProjectAdmins"
                  |             WHEN "ProjectClients" IN prs THEN "ProjectClients"
                  |             WHEN "ProjectContacts" IN prs THEN "ProjectContacts"
                  |             ELSE "None"
                  |        END
                  |    } AS r
                  |    WITH x AS f, r
                  |    RETURN 1""".stripMargin

    val result = graph.execute(s"EXPLAIN $query")

    println(result.getExecutionPlanDescription)
  }

  test("fuuuuse") {
    val query = """
      |WITH 1 as one
      |WITH 2 as two
      |WITH 3 as three
      |RETURN 1
    """.stripMargin
    val result = graph.execute(s"EXPLAIN $query")

    println(result.getExecutionPlanDescription)
  }

  test("Using keys() function with NODE Not Empty result") {

    val n = createNode(Map("name" -> "Andres", "surname" -> "Lopez"))

    val result = execute("match (n) where id(n) = " + n.getId + " unwind (keys(n)) AS x return distinct(x) as theProps")

    result.columnAs[String]("theProps").toList should equal(List("name","surname"))
  }

  test("Using keys() function with MULTIPLE_NODES Not-Empty result") {

    val n1 = createNode(Map("name" -> "Andres", "surname" -> "Lopez"))
    val n2 = createNode(Map("otherName" -> "Andres", "otherSurname" -> "Lopez"))

    val result = execute("match (n) where id(n) = " + n1.getId + " or id(n) = " + n2.getId + " unwind (keys(n)) AS x return distinct(x) as theProps")

    result.columnAs[String]("theProps").toList should equal(List("name","surname","otherName","otherSurname"))
  }

  test("Using keys() function with NODE Empty result") {

    val n = createNode()

    val result = execute("match (n) where id(n) = " + n.getId() + " unwind (keys(n)) AS x return distinct(x) as theProps")

    result.columnAs[String]("theProps").toList should equal(List())
  }


  test("Using keys() function with NODE NULL result") {

    val n = createNode()

    val result = execute("optional match (n) where id(n) = " + n.getId() + " unwind (keys(n)) AS x return distinct(x) as theProps")

    result.columnAs[String]("theProps").toList should equal(List())
  }

  test("Using keys() function with RELATIONSHIP Not-Empty result") {

    val r = relate(createNode(), createNode(), "KNOWS", Map("level" -> "bad", "year" -> "2015"))

    val result = execute("match ()-[r:KNOWS]-() where id(r) = " + r.getId + " unwind (keys(r)) AS x return distinct(x) as theProps")

    result.columnAs[String]("theProps").toList should equal(List("level","year"))
  }

  test("Using keys() function with RELATIONSHIP Empty result") {

    val r = relate(createNode(), createNode(), "KNOWS")

    val result = execute("match ()-[r:KNOWS]-() where id(r) = " + r.getId + " unwind (keys(r)) AS x return distinct(x) as theProps")

    result.columnAs[String]("theProps").toList  should equal(List())
  }

  test("Using keys() function with RELATIONSHIP NULL result") {

    val r = relate(createNode(), createNode(), "KNOWS")

    val result = execute("optional match ()-[r:KNOWS]-() where id(r) = " + r.getId + " unwind (keys(r)) AS x return distinct(x) as theProps")

    result.columnAs[String]("theProps").toList should equal(List())
  }

  test("Using keys() on literal maps") {

    val result = execute("""return keys({name:'Alice', age:38, address:{city:'London', residential:true}}) as k""")

    result.toList should equal(List(Map("k" -> Seq("name", "age", "address"))))
  }

  test("Using keys() with map from parameter") {

    val result = execute("""return keys({param}) as k""",
      "param"->Map("name" -> "Alice", "age" -> 38, "address" -> Map("city" -> "London", "residential" -> true)))

    result.toList should equal(List(Map("k" -> Seq("name", "age", "address"))))
  }
}
