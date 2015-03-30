package org.neo4j.cypher.internal.compiler.v2_2.executionplan

import org.neo4j.cypher.internal.commons.CypherFunSuite

class EffectsTest extends CypherFunSuite {

  test("logical and works") {
    val first = Effects(WritesRelationships, ReadsProperty("2"))
    val second = Effects(WritesRelationships, WritesProperty("2"))

    (first & second).effectsSet should contain only WritesRelationships
  }

  test("logical and considers equal property ids") {
    val first = Effects(WritesRelationships, ReadsProperty("2"))
    val second = Effects(ReadsProperty("2"))
    (first & second).effectsSet should contain only ReadsProperty("2")

  }

  test("logical or works") {
    val first = Effects(WritesRelationships, ReadsLabel(""))
    val second = Effects(ReadsNodes, ReadsLabel(""))

    (first | second).effectsSet should contain only(WritesRelationships, ReadsLabel(""), ReadsNodes)
  }

  test("logical or works 2") {
    val first = Effects.READ_EFFECTS
    val second = Effects(ReadsLabel("foo"))

    (first | second).effectsSet should contain only(ReadsNodes, ReadsLabel("foo"), ReadsLabel(""), ReadsProperty(""), ReadsRelationships)

  }
}
