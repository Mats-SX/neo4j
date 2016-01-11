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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans

import org.neo4j.cypher.internal.frontend.v3_0.ast.Expression
import org.neo4j.cypher.internal.compiler.v3_0.planner.{CardinalityEstimation, PlannerQuery}

case class LetSelectOrSemiApply(left: LogicalPlan, right: LogicalPlan, idName: IdName, expr: Expression)
                               (val solved: PlannerQuery with CardinalityEstimation)
  extends AbstractLetSelectOrSemiApply(left, right, idName, expr, solved) {

  override def mapExpressions(f: (Set[IdName], Expression) => Expression): LogicalPlan =
    copy(expr = f(left.availableSymbols, expr))(solved)

  override def newWithChildren(newLhs: Option[LogicalPlan], newRhs: Option[LogicalPlan]): LogicalPlan =
    copy(left = newLhs.getOrElse(throw new IllegalStateException))(solved)

}

case class LetSelectOrAntiSemiApply(left: LogicalPlan, right: LogicalPlan, idName: IdName, expr: Expression)
                                   (val solved: PlannerQuery with CardinalityEstimation)
  extends AbstractLetSelectOrSemiApply(left, right, idName, expr, solved) {

  override def mapExpressions(f: (Set[IdName], Expression) => Expression): LogicalPlan =
    copy(expr = f(left.availableSymbols, expr))(solved)

  override def newWithChildren(newLhs: Option[LogicalPlan], newRhs: Option[LogicalPlan]): LogicalPlan =
    copy(left = newLhs.getOrElse(throw new IllegalStateException))(solved)

}

abstract class AbstractLetSelectOrSemiApply(left: LogicalPlan, right: LogicalPlan, idName: IdName, expr: Expression,
                                            solved: PlannerQuery with CardinalityEstimation)
  extends LogicalPlan with LazyLogicalPlan {
  val lhs = Some(left)
  val rhs = Some(right)

  def availableSymbols = left.availableSymbols + idName
}
