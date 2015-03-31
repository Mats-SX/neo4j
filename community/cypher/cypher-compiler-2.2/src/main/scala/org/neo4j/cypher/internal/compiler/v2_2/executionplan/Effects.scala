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
package org.neo4j.cypher.internal.compiler.v2_2.executionplan

import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_2.commands.{ReturnItem, SortItem}
import org.neo4j.cypher.internal.compiler.v2_2.mutation.{Effectful, UpdateAction}
import org.neo4j.cypher.internal.compiler.v2_2.symbols.SymbolTable

case class Effects(effectsSet: Set[Effect] = Set.empty) {

  def &(other: Effects): Effects = Effects(other.effectsSet.intersect(effectsSet))

  def |(other: Effects): Effects = Effects(effectsSet ++ other.effectsSet)

  def contains(effect: Effect): Boolean = effectsSet(effect)

  def reads() = effectsSet.exists(_.reads)

  def writes() = effectsSet.exists(_.writes)
}

object WriteEffects extends Effects(Set(WritesNodes, WritesRelationships, WritesLabels, WritesProperties)) {
  override def toString = "WRITE EFFECTS"
}

object ReadEffects extends Effects(Set(ReadsNodes, ReadsRelationships, ReadsLabels, ReadsProperties)) {
  override def toString = "READ EFFECTS"
}

object AllEffects extends Effects((WriteEffects | ReadEffects).effectsSet) {
  override def toString = "ALL EFFECTS"
}

object Effects {

  def apply(effectsSeq: Effect*): Effects = Effects(effectsSeq.toSet)

  implicit class TraversableEffects(iter: Traversable[Effectful]) {
    def effects: Effects = Effects(iter.flatMap(_.effects.effectsSet).toSet)
  }

  implicit class TraversableExpressions(iter: Traversable[Expression]) {
    def effects: Effects = Effects(iter.flatMap(_.effects.effectsSet).toSet)
  }

  implicit class EffectfulReturnItems(iter: Traversable[ReturnItem]) {
    def effects: Effects = Effects(iter.flatMap(_.expression.effects.effectsSet).toSet)
  }

  implicit class EffectfulUpdateAction(commands: Traversable[UpdateAction]) {
    def effects(symbols: SymbolTable): Effects = Effects(commands.flatMap(_.effects(symbols).effectsSet).toSet)
  }

  implicit class MapEffects(m: Map[_, Expression]) {
    def effects: Effects = Effects(m.values.flatMap(_.effects.effectsSet).toSet)
  }

  implicit class SortItemEffects(m: Traversable[SortItem]) {
    def effects: Effects = Effects(m.flatMap(_.expression.effects.effectsSet).toSet)
  }

}

trait Effect {
  def reads: Boolean

  def writes: Boolean
}

protected trait ReadEffect extends Effect {
  override def reads = true

  override def writes = false
}

protected trait WriteEffect extends Effect {
  override def reads = false

  override def writes = true
}

case object ReadsNodes extends ReadEffect {
  override def toString = "READ NODES"
}

case object WritesNodes extends WriteEffect {
  override def toString = "WRITES NODES"
}

case object ReadsRelationships extends ReadEffect {
  override def toString = "READS RELATIONSHIPS"
}

case object WritesRelationships extends WriteEffect {
  override def toString = "WRITES RELATIONSHIPS"
}

case class ReadsProperty(propertyName: String) extends ReadEffect {
  override def toString = s"READS PROPERTY '$propertyName'"
}

object ReadsProperties extends ReadsProperty("") {
  override def toString = "READS PROPERTIES"
}

case class WritesProperty(propertyName: String) extends WriteEffect {
  override def toString = s"WRITES PROPERTY '$propertyName'"
}

object WritesProperties extends WritesProperty("") {
  override def toString = "WRITES PROPERTIES"
}

case class ReadsLabel(labelName: String) extends ReadEffect {
  override def toString = s"READS LABEL '$labelName'"
}

object ReadsLabels extends ReadsLabel("") {
  override def toString = "READS LABELS"
}

case class WritesLabel(labelName: String) extends WriteEffect {
  override def toString = s"WRITES LABEL '$labelName'"
}

object WritesLabels extends WritesLabel("") {
  override def toString = "WRITES LABELS"
}
