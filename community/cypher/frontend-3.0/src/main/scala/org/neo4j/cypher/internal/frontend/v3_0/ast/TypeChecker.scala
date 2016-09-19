package org.neo4j.cypher.internal.frontend.v3_0.ast

import org.neo4j.cypher.internal.frontend.v3_0.SemanticCheckResult._
import org.neo4j.cypher.internal.frontend.v3_0.symbols.{CypherType, TypeSpec}
import org.neo4j.cypher.internal.frontend.v3_0.{SemanticCheckResult, SemanticError, SemanticState, _}

object TypeChecker {
  case class Signature(argumentTypes: IndexedSeq[CypherType], outputType: CypherType)

  def apply(signatures: Seq[Signature], arguments: Seq[Expression],
            specifyType: (=> TypeSpec) => (SemanticState) => Either[SemanticError, SemanticState]): SemanticCheck = s => {
    val initSignatures = signatures.filter(_.argumentTypes.length == arguments.length)

    val copy = arguments

    val (remainingSignatures: Seq[Signature], result) = arguments.foldLeft((initSignatures, success(s))) {
      case (accumulator@(Seq(), _), _) =>
        accumulator
      case ((possibilities, r1), arg)  =>
        val argTypes = possibilities.foldLeft(TypeSpec.none) { _ | _.argumentTypes.head.covariant }
        val r2 = arg.expectType(argTypes)(r1.state)

        val actualTypes = arg.types(r2.state)
        val remainingPossibilities = possibilities.filter {
          sig => actualTypes containsAny sig.argumentTypes.head.covariant
        } map {
          sig => sig.copy(argumentTypes = sig.argumentTypes.tail)
        }
        (remainingPossibilities, SemanticCheckResult(r2.state, r1.errors ++ r2.errors))
    }

    val outputType = remainingSignatures match {
      case Seq() => TypeSpec.all
      case _     => remainingSignatures.foldLeft(TypeSpec.none) { _ | _.outputType.invariant }
    }
    specifyType(outputType)(result.state) match {
      case Left(err)    => SemanticCheckResult(result.state, result.errors :+ err)
      case Right(state) => SemanticCheckResult(state, result.errors)
    }
  }
}
