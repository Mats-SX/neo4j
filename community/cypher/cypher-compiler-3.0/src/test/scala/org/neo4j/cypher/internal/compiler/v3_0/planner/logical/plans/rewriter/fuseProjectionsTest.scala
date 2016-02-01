package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.v3_0.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{Eager, LogicalPlan, Projection}
import org.neo4j.cypher.internal.frontend.v3_0.ast.{Expression, False, True, Variable}
import org.neo4j.cypher.internal.frontend.v3_0.helpers.fixedPoint
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class fuseProjectionsTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should fuse two nested projections") {
    val leaf = newMockedLogicalPlan()
    val p1 = Projection(leaf, Map.empty)(solved)
    val p2 = Projection(p1, Map.empty)(solved)

    rewrite(p2) should equal(Projection(leaf, Map.empty)(solved))
  }

  test("should fuse three nested projections") {
    val leaf = newMockedLogicalPlan()
    val p1 = Projection(leaf, Map.empty)(solved)
    val p2 = Projection(p1, Map.empty)(solved)
    val p3 = Projection(p2, Map.empty)(solved)

    rewrite(p3) should equal(Projection(leaf, Map.empty)(solved))
  }

  test("should keep only the outer-most projected variables") {
    val leaf = newMockedLogicalPlan()
    val p1 = Projection(leaf, Map("a" -> fakeExpr()))(solved)
    val p2 = Projection(p1, Map.empty)(solved)
    val p3 = Projection(p2, Map("b" -> fakeExpr()))(solved)

    rewrite(p3) should equal(Projection(leaf, Map("b" -> fakeExpr()))(solved))
  }

  test("should overwrite projected variables and keep the outer-most value") {
    val leaf = newMockedLogicalPlan()
    val p1 = Projection(leaf, Map("a" -> fakeExpr()))(solved)
    val p2 = Projection(p1, Map("b" -> fakeExpr(false)))(solved)
    val p3 = Projection(p2, Map("b" -> fakeExpr()))(solved)

    rewrite(p3) should equal(Projection(leaf, Map("b" -> fakeExpr()))(solved))
  }

  test("should work when projections project onto eachother") {
    val leaf = newMockedLogicalPlan()
    val p1 = Projection(leaf, Map("a" -> fakeExpr()))(solved)
    val p2 = Projection(p1, Map("b" -> Variable("a")(pos)))(solved)

    rewrite(p2) should equal(Projection(leaf, Map("b" -> fakeExpr()))(solved))
  }

  test("should not fuse projections that are not directly nested") {
    val leaf = newMockedLogicalPlan()
    val p1 = Projection(leaf, Map("a" -> fakeExpr()))(solved)
    val notP = Eager(p1)(solved)
    val p3 = Projection(notP, Map("b" -> fakeExpr()))(solved)

    rewrite(p3) should equal(p3)
  }

  private def fakeExpr(value: Boolean = true): Expression = if (!value) True()(pos) else False()(pos)

  private def rewrite(p: LogicalPlan): LogicalPlan =
    fixedPoint((p: LogicalPlan) => p.endoRewrite(fuseProjections))(p)

}
