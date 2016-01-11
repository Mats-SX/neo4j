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
package org.neo4j.cypher.internal.compiler.v3_0.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.{SemanticState, Rewriter, topDown}
import org.neo4j.helpers.ThisShouldNotHappenError

case class addMergeHorizons(state: SemanticState) extends Rewriter {

  override def apply(that: AnyRef): AnyRef = topDown(instance)(that)


  private val instance: Rewriter = Rewriter.lift {
    case q @ SingleQuery(clauses) =>
      q.copy(clauses = clauses.foldLeft(Seq.empty[Clause]) { (rewritten, clause) =>
        clause match {
          case m:Merge if !(rewritten.last.isInstanceOf[With]) =>
            rewritten ++ Seq(
              With(distinct = false, returnItems(m), None, None, None, None)(m.position),
              m
            )
          case _ => rewritten :+ clause
        }
      })(q.position)

  }

  private def returnItems(clause: Clause): ReturnItems = {
    val scope = state.scope(clause).getOrElse {
      throw new ThisShouldNotHappenError("GOD", s"${clause.name} should note its Scope in the SemanticState")
    }

    val clausePos = clause.position
    val symbolNames = scope.symbolNames
    val expandedItems = symbolNames.toSeq.sorted.map { id =>
      val idPos = scope.symbolTable(id).definition.position
      val expr = Variable(id)(idPos)
      val alias = expr.copyId
      AliasedReturnItem(expr, alias)(clausePos)
    }

    ReturnItems(includeExisting = false, expandedItems)(clausePos)
  }

}
