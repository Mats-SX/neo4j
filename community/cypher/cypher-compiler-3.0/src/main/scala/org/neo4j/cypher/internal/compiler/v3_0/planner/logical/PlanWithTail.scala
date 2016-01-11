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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical

import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{Expand, LogicalPlan, NodeLogicalLeafPlan}
import org.neo4j.cypher.internal.compiler.v3_0.planner._
import org.neo4j.cypher.internal.frontend.v3_0.Rewriter

/*
This class ties together disparate query graphs through their event horizons. It does so by using Apply,
which in most cases is then rewritten away by LogicalPlan rewriting.

In cases where the preceding PlannerQuery has updates we must make the Apply an EagerApply if there
are overlaps between the previous update and the reads of any of the tails. We must also make Reads
in the tail into RepeatableReads if there is overlap between the read and update within the PlannerQuery.

For example:

    +Apply2
    |\
    | +Update3
    | |
    | +Read3
    |
    +Apply1
    |\
    | +Update2
    | |
    | +Read2
    |
    +Update1
    |
    +Read1

In this case the following must hold
  - Apply1 is eager if updates from Update1 will be matched by Reads2 or Reads3.
  - Apply2 is eager if updates from Update2 will be matched by Reads3
  - If Update2 affects Read2, Read2 must use RepeatableRead
  - If Update3 affects Read3, Read2 must use RepeatableRead

*/
case class PlanWithTail(expressionRewriterFactory: (LogicalPlanningContext => Rewriter) = ExpressionRewriterFactory,
                        planEventHorizon: LogicalPlanningFunction2[PlannerQuery, LogicalPlan, LogicalPlan] = PlanEventHorizon,
                        planPart: (PlannerQuery, LogicalPlanningContext, Option[LogicalPlan]) => LogicalPlan = planPart,
                        planUpdates: LogicalPlanningFunction2[PlannerQuery, LogicalPlan, LogicalPlan] = PlanUpdates)
  extends LogicalPlanningFunction2[LogicalPlan, Option[PlannerQuery], LogicalPlan] {

  override def apply(lhs: LogicalPlan, remaining: Option[PlannerQuery])(implicit context: LogicalPlanningContext): LogicalPlan = {
    remaining match {
//      case Some(merge: MergePlannerQuery) =>

//      case Some(plannerQuery @ RegularPlannerQuery(QueryGraph.empty,
//                                                   UpdateGraph.empty,
//                                                   QueryProjection.empty,
//                                                   tail)) =>
//        val newLhs = lhs.updateSolved(x => context.logicalPlanProducer.estimatePlannerQuery(x.updateTail(_.withTail(plannerQuery))))
//        apply(newLhs, tail)(context)

      case Some(plannerQuery) =>
        val lhsContext = context.recurse(lhs)
        // TODO: REV: Why is countStorePlanner not used here?
        val partPlan = planPart(plannerQuery, lhsContext, None)

        ///use eager if configured to do so
        val alwaysEager = context.config.updateStrategy.alwaysEager
        //If reads interfere with writes, make it a RepeatableRead
        var shouldPlanEagerBeforeTail = false
        val planWithEffects =
          if (!plannerQuery.isInstanceOf[MergePlannerQuery] &&
            (alwaysEager || Eagerness.conflictInTail(plannerQuery, plannerQuery))) {
            // With some selections we cannot apply the repeatable read. This is a pessimistic approach (any predicates)
            //if (plannerQuery.queryGraph.selections.predicates.nonEmpty)
            if (true)
              context.logicalPlanProducer.planEager(partPlan)
            else
              planRepeatableRead(partPlan)
          }
          else if (plannerQuery.isInstanceOf[MergePlannerQuery] &&
            (alwaysEager ||
//              Eagerness.conflictInTail(partPlan, plannerQuery))) {
              (plannerQuery.tail.isDefined &&
               Eagerness.conflictInTail(plannerQuery, plannerQuery.tail.get)))) {
            // For a MergePlannerQuery the merge have to be able to read its own writes,
            // so we need to plan an eager between this and its tail instead
            shouldPlanEagerBeforeTail = true
            partPlan
          }
          else partPlan

//          if (alwaysEager || Eagerness.conflictInTail(partPlan, plannerQuery))
//            context.logicalPlanProducer.planRepeatableRead(partPlan)
//          else partPlan

        val planWithUpdates = planUpdates(plannerQuery, planWithEffects)(context)


        Debug.dprintln(s"PlanWithTail planWithUpdates: solved=${planWithUpdates.solved}\nplan=$planWithUpdates")

        //If writes in previous PlannerQuery interferes with any of the reads here or in tail, we need to be eager
        val applyPlan = {
          val lastPlannerQuery = lhs.solved.last
//          val newLhs = if (alwaysEager ||
//            (!lastPlannerQuery.writeOnly && plannerQuery.allQueryGraphs.exists(lastPlannerQuery.updateGraph.overlaps))
//            || plannerQuery.allUpdateGraphs.exists(lastPlannerQuery.updateGraph.deleteOverlapWithMergeNodeIn))
//              context.logicalPlanProducer.planEager(lhs)
//            else lhs
            //if (!plannerQuery.isInstanceOf[MergePlannerQuery] && // Skip this eagerness for MergePlannerQuery
          val newLhs =
            if (alwaysEager ||
              plannerQuery.allUpdateGraphs.exists(lastPlannerQuery.updateGraph.deleteOverlapWithMergeNodeIn) ||
              (!lastPlannerQuery.writeOnly && !plannerQuery.isInstanceOf[MergePlannerQuery] &&
                plannerQuery.allQueryGraphs.exists(lastPlannerQuery.updateGraph.overlaps)))
              context.logicalPlanProducer.planEager(lhs)
            else lhs

          context.logicalPlanProducer.planTailApply(newLhs, planWithUpdates)
        }

        Debug.dprintln(s"PlanWithTail applyPlan: solved=${applyPlan.solved}\nplan=$applyPlan")

        val eagerPlan =
          if (shouldPlanEagerBeforeTail)
            context.logicalPlanProducer.planEager(applyPlan)
          else applyPlan

        val applyContext = lhsContext.recurse(eagerPlan)
        val projectedPlan = planEventHorizon(plannerQuery, eagerPlan)(applyContext)
        val projectedContext = applyContext.recurse(projectedPlan)

        val completePlan = {
          val expressionRewriter = expressionRewriterFactory(projectedContext)

          projectedPlan.endoRewrite(expressionRewriter)
        }

        // planning nested expressions doesn't change outer cardinality
        apply(completePlan, plannerQuery.tail)(projectedContext)


      case None =>
        lhs
    }
  }

  private def planRepeatableRead(plan: LogicalPlan)(implicit context: LogicalPlanningContext): LogicalPlan = {

    def isLeaf(p: LogicalPlan) = p match {
      case _: NodeLogicalLeafPlan => true
      case Expand(_, _, _, _, _, _, _) => true
      case _ => false
    }

    def treeRewrite(p: LogicalPlan): Option[LogicalPlan] = {

      (p.lhs, p.rhs) match {
        case (Some(lhs), None) if isLeaf(lhs) =>
          Some(p.newWithChildren(newLhs = Some(context.logicalPlanProducer.planRepeatableRead(lhs)), None))

        case (Some(lhs), None) =>
          Some(p.newWithChildren(newLhs = treeRewrite(lhs), None))

        case (Some(lhs: NodeLogicalLeafPlan), Some(rhs: NodeLogicalLeafPlan)) =>
          Some(p.newWithChildren(newLhs = Some(context.logicalPlanProducer.planRepeatableRead(lhs)),
                                 newRhs = Some(context.logicalPlanProducer.planRepeatableRead(rhs))))
        case (Some(lhs: NodeLogicalLeafPlan), Some(rhs)) =>
          Some(p.newWithChildren(newLhs = Some(context.logicalPlanProducer.planRepeatableRead(lhs)),
                                 newRhs = treeRewrite(rhs)))
        case (Some(lhs), Some(rhs: NodeLogicalLeafPlan)) =>
          Some(p.newWithChildren(newLhs = treeRewrite(lhs),
                                 newRhs = Some(context.logicalPlanProducer.planRepeatableRead(rhs))))

        case (None, None) =>
          Some(p)

        case (Some(l), Some(r)) =>
          Some(p.newWithChildren(treeRewrite(l), treeRewrite(r)))
      }
    }

//    plan

    plan match {
      case leaf: NodeLogicalLeafPlan =>
        context.logicalPlanProducer.planRepeatableRead(plan)
      case other =>
        treeRewrite(other).getOrElse(throw new IllegalStateException("w00t"))
    }
  }
}
