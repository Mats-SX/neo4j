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

import org.neo4j.cypher.internal.compiler.v2_2.pipes.{EagerPipe, Pipe}

object addEagernessIfNecessary extends (Pipe => Pipe) {
  def wouldInterfere(from: Effects, to: Effects): Boolean = {
    val nodesInterfere = from.contains(ReadsNodes) && to.contains(WritesNodes)
    val relsInterfere = from.contains(ReadsRelationships) && to.contains(WritesRelationships)

    nodesInterfere || relsInterfere || propertiesInterfere(from, to) || labelsInterfere(from, to)
  }

  def apply(toPipe: Pipe): Pipe = {
    val sources = toPipe.sources.map(apply).map { fromPipe =>
      val from = fromPipe.effects
      val to = toPipe.localEffects
      if (wouldInterfere(from, to)) {
        new EagerPipe(fromPipe)(fromPipe.monitor)
      } else {
        fromPipe
      }
    }
    toPipe.dup(sources.toList)
  }

  private def propertiesInterfere(from: Effects, to: Effects): Boolean = {
    val readMap: Map[String, ReadsProperty] = from.effectsSet.collect {
      case property@ReadsProperty(_) => property
    }.map(x => (x.propertyName, x)).toMap

    val writes = to.effectsSet.collect {
      case property@WritesProperty(_) => property
    }

    val propertiesInterfere =
      (readMap.contains("") && writes.nonEmpty) ||
        (writes.contains(WritesProperty("")) && readMap.nonEmpty) ||
        writes.exists(x => readMap.contains(x.propertyName))
    propertiesInterfere
  }

  private def labelsInterfere(from: Effects, to: Effects): Boolean = {
    val readMap: Map[String, ReadsLabel] = from.effectsSet.collect {
      case label@ReadsLabel(_) => label
    }.map(x => (x.labelName, x)).toMap

    val writes = to.effectsSet.collect {
      case label@WritesLabel(_) => label
    }

    val propertiesInterfere =
      (readMap.contains("") && writes.nonEmpty) ||
        (writes.contains(WritesLabel("")) && readMap.nonEmpty) ||
        writes.exists(x => readMap.contains(x.labelName))
    propertiesInterfere
  }
}
