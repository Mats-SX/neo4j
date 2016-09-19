package org.neo4j.cypher.internal.frontend.v3_0.ast

import org.neo4j.cypher.internal.frontend.v3_0.ast.TypeChecker.Signature
import org.neo4j.cypher.internal.frontend.v3_0.symbols._
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_0.{SemanticError, SemanticState}

class TypeCheckerTest extends CypherFunSuite {

  test("should accept a specified type") {
    testThat(Seq(Signature(Vector(CTInteger), CTInteger)), Seq(DummyExpression(CTInteger)), CTInteger)
    testThat(Seq(Signature(Vector(CTInteger), CTString)), Seq(DummyExpression(CTInteger)), CTString)
    testThat(Seq(Signature(Vector(CTInteger), CTString),
                    Signature(Vector(CTFloat), CTBoolean)), Seq(DummyExpression(CTFloat)), CTString, CTBoolean)
  }

  test("any type") {
    testThat(Seq(Signature(Vector(CTInteger), CTBoolean),
                 Signature(Vector(CTFloat), CTFloat)), Seq(DummyExpression(CTAny)), CTBoolean, CTFloat)
  }

  test("toInt") {
    val signatures = Seq(Signature(Vector(CTString), CTInteger),
                         Signature(Vector(CTNumber), CTInteger))
    testThat(signatures, Seq(DummyExpression(CTAny)), CTInteger)
    testThat(signatures, Seq(DummyExpression(CTFloat)), CTInteger)
    testThat(signatures, Seq(DummyExpression(CTNumber)), CTInteger)
    testThat(signatures, Seq(DummyExpression(CTInteger)), CTInteger)
  }


  private def testThat(signatures: Seq[Signature], arguments: Seq[Expression], types: CypherType*) = {
    TypeChecker(signatures, arguments, assertTypeIs(types)).apply(SemanticState.clean)
  }

  private def assertTypeIs(expected: Seq[CypherType]): (=> TypeSpec) => SemanticState => Either[SemanticError, SemanticState] = actual => {
    actual should equal(TypeSpec.exact(expected))
    s => Right(s)
  }


}
