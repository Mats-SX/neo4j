package org.neo4j.cypher.internal.compiler.v3_2.phases

import org.neo4j.cypher.internal.frontend.v3_2.phases.BaseContext

trait FooTransformer[-C, -S] {
  def transform[FROM <: S, TO <: FROM](from: FROM, context: C): TO

  def andThen[D <: C, FROM <: S, TO <: FROM, RESULT <: TO](other: FooTransformer[D, TO]): FooTransformer[D, RESULT] =
    AndThen[D, RESULT](this, other)
}

case class FooIf[C, S](f: S => Boolean)(ifTrue: FooTransformer[C, S], ifFalse: FooTransformer[C, S]) extends FooTransformer[C, S] {
  override def transform[FROM <: S, TO <: FROM](from: FROM, context: C): TO = {
    if (f(from))
      ifTrue.transform(from, context)
    else
      ifFalse.transform(from, context)
  }
}

case class AndThen[C, S](step1: FooTransformer[C, S], step2: FooTransformer[C, S]) extends FooTransformer[C, S] {
  override def transform[FROM <: S, TO <: FROM](from: FROM, context: C): TO = {
    val f = transform2(from, context)
    f
  }

  private def transform2[FROM1 <: S, TO1 <: FROM1, FROM2 <: TO1, TO2 <: FROM2](from: FROM1, context: C): TO2 = {
    val result1 = step1.transform[FROM1, TO1](from, context)
    val result2 = step2.transform[TO1, TO2](result1, context)
    result2
  }
}

//case class If[C, S](f: CompilationState => Boolean)(thenT: FooTransformer[C]) extends FooTransformer[C] {
//  override def transform(from: CompilationState, context: C): CompilationState = {
//    if (f(from))
//      thenT.transform(from, context)
//    else
//      from
//  }
//}

trait A
trait B extends A
trait B2 extends B
trait C extends A

trait Context

class T1 extends FooTransformer[Context, A] {
  override def transform[FROM <: A, TO <: FROM](from: FROM, context: Context): TO = {
    new B {}
  }
}

class T2 extends FooTransformer[Context, B] {
  override def transform[FROM <: B, TO <: FROM](from: FROM, context: Context): TO = {
    new B2 {}
  }
}
