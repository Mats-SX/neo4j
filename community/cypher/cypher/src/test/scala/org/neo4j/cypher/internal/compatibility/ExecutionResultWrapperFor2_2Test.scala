package org.neo4j.cypher.internal.compatibility

import java.util

import org.hamcrest.MatcherAssert._
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers._
import org.junit.Assert._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.PlannerName
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.InternalExecutionResult
import org.neo4j.cypher.javacompat.ExecutionResult
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.graphdb.{ResourceIterator, Result, Relationship, Node}
import org.neo4j.kernel.impl.query.{QuerySession, QueryExecutionMonitor}
import org.neo4j.kernel.impl.util.ResourceIterators

import scala.collection.mutable.ListBuffer

/**
 * Created by mats on 26/03/15.
 */
class ExecutionResultWrapperFor2_2Test extends CypherFunSuite {

  test("visitor get works") {
    val n = mock[Node]
    val r = mock[Relationship]
    val objectUnderTest = createInnerExecutionResult("a", "a", n, r)

    val result = ListBuffer[AnyRef]()

    objectUnderTest.accept(new ResultVisitor {
      override def visit(row: ResultRow): Boolean = {
        result += row.get("a")
        true
      }
    })

    result should contain allOf("a", n, r)
  }

  def createInnerExecutionResult(column: String, values: AnyRef*) = {
    val mockObj = mock[InternalExecutionResult]
    var offset = 0
    when(mockObj.hasNext).thenAnswer(new Answer[Boolean] {
      override def answer(invocationOnMock: InvocationOnMock): Boolean = offset < values.length
    })
    when(mockObj.next()).thenAnswer(new Answer[Map[String, AnyRef]] {
      override def answer(invocationOnMock: InvocationOnMock): Map[String, AnyRef] = {
        val result = Map(column -> values.take(offset))
        offset += 1
       result
      }
    })
    when(mockObj.javaIterator).thenReturn(mock[ResourceIterator[java.util.Map[String, Any]]])
    new ExecutionResultWrapperFor2_2(mockObj, mock[PlannerName])(mock[QueryExecutionMonitor], mock[QuerySession])
  }

//  private ExecutionResult createInnerExecutionResult( final String column, final Object... values )
//  {
//    InternalExecutionResult mock = mock(InternalExecutionResult.class);
//    final AtomicInteger offset = new AtomicInteger();
//    when(mock.hasNext()).thenReturn(offset.get() < values.length);
//    when(mock.next()).thenAnswer(new Answer<Map<String , Object>>() {
//      @Override
//      public Map<String, Object> answer(InvocationOnMock invocationOnMock) throws Throwable {
//        Map<String, Object> result = new HashMap<>();
//        result.put( column, values[offset.get()] );
//        offset.incrementAndGet();
//        return result;
//      }
//    });
//    when(mock.javaIterator()).thenReturn(ResourceIterators.EMPTY_ITERATOR);
//    ExtendedExecutionResult inner = new ExecutionResultWrapperFor2_3(mock, mock(PlannerName.class), mock(QueryExecutionMonitor.class), mock(QuerySession.class));
//    return new ExecutionResult(inner);
//  }
//  @Test
//  @throws(classOf[Exception])
//  def visitor_get_works {
//    val n: Node = mock(classOf[Node])
//    val r: Relationship = mock(classOf[Relationship])
//    val objectUnderTest: ExecutionResult = createInnerExecutionResult("a", "a", n, r)
//    val results: List[AnyRef] = new ArrayList[AnyRef]
//    objectUnderTest.accept( new class null {
//      def visit(row: Result.ResultRow): Boolean = {
//        results.add(row.get("a"))
//        return true
//      }
//    })
//    assertThat(results, containsInAnyOrder("a", n, r))
//  }
//
//  @Test
//  @throws(classOf[Exception])
//  def visitor_get_string_works {
//    val objectUnderTest: ExecutionResult = createInnerExecutionResult("a", "a", "b", "c")
//    val results: List[String] = new ArrayList[String]
//    objectUnderTest.accept( new class null {
//      def visit(row: Result.ResultRow): Boolean = {
//        results.add(row.getString("a"))
//        return true
//      }
//    })
//    assertThat(results, containsInAnyOrder("a", "b", "c"))
//  }
//
//  @Test
//  @throws(classOf[Exception])
//  def visitor_get_node_works {
//    val n1: Node = mock(classOf[Node])
//    val n2: Node = mock(classOf[Node])
//    val n3: Node = mock(classOf[Node])
//    val objectUnderTest: ExecutionResult = createInnerExecutionResult("a", n1, n2, n3)
//    val results: List[Node] = new ArrayList[Node]
//    objectUnderTest.accept( new class null {
//      def visit(row: Result.ResultRow): Boolean = {
//        results.add(row.getNode("a"))
//        return true
//      }
//    })
//    assertThat(results, containsInAnyOrder(n1, n2, n3))
//  }
//
//  @Test
//  @throws(classOf[Exception])
//  def when_asking_for_a_node_when_it_is_not_a_node {
//    val objectUnderTest: ExecutionResult = createInnerExecutionResult("a", 42)
//    val a: Result.ResultVisitor = new class null {
//      def visit(row: Result.ResultRow): Boolean = {
//        row.getNode("a")
//        return true
//      }
//    }
//    try {
//      objectUnderTest.accept(a)
//      fail("Expected an exception")
//    }
//    catch {
//      case e: NoSuchElementException => {
//        assertEquals(e.getMessage, "The current item in column \"a\" is not a Node")
//      }
//    }
//  }
//
//  @Test
//  @throws(classOf[Exception])
//  def when_asking_for_a_non_existing_column_throws {
//    val objectUnderTest: ExecutionResult = createInnerExecutionResult("a", 42)
//    val a: Result.ResultVisitor = new class null {
//      def visit(row: Result.ResultRow): Boolean = {
//        row.getNode("does not exist")
//        return true
//      }
//    }
//    try {
//      objectUnderTest.accept(a)
//      fail("Expected an exception")
//    }
//    catch {
//      case e: IllegalArgumentException => {
//        assertEquals(e.getMessage, "No column \"does not exist\" exists")
//      }
//    }
//  }
//
//  @Test
//  @throws(classOf[Exception])
//  def when_asking_for_a_rel_when_it_is_not_a_rel {
//    val objectUnderTest: ExecutionResult = createInnerExecutionResult("a", 42)
//    val a: Result.ResultVisitor = new class null {
//      def visit(row: Result.ResultRow): Boolean = {
//        row.getRelationship("a")
//        return true
//      }
//    }
//    try {
//      objectUnderTest.accept(a)
//      fail("Expected an exception")
//    }
//    catch {
//      case e: NoSuchElementException => {
//        assertEquals(e.getMessage, "The current item in column \"a\" is not a Relationship")
//      }
//    }
//  }
//
//  @Test
//  @throws(classOf[Exception])
//  def null_key_gives_a_friendly_error {
//    val objectUnderTest: ExecutionResult = createInnerExecutionResult("a", 42)
//    val a: Result.ResultVisitor = new class null {
//      def visit(row: Result.ResultRow): Boolean = {
//        row.getNumber(null)
//        return true
//      }
//    }
//    try {
//      objectUnderTest.accept(a)
//      fail("Expected an exception")
//    }
//    catch {
//      case e: IllegalArgumentException => {
//        assertEquals(e.getMessage, "No column \"null\" exists")
//      }
//    }
//  }
//
//  @Test
//  @throws(classOf[Exception])
//  def when_asking_for_a_null_value_nothing_bad_happens {
//    val objectUnderTest: ExecutionResult = createInnerExecutionResult("a", null.asInstanceOf[AnyRef])
//    val result: List[Relationship] = new ArrayList[Relationship]
//    val a: Result.ResultVisitor = new class null {
//      def visit(row: Result.ResultRow): Boolean = {
//        result.add(row.getRelationship("a"))
//        return true
//      }
//    }
//    objectUnderTest.accept(a)
//    assertThat(result, contains(null.asInstanceOf[Relationship]))
//  }
//
//  @Test
//  @throws(classOf[Exception])
//  def stop_on_return_false {
//    val objectUnderTest: ExecutionResult = createInnerExecutionResult("a", 1l, 2l, 3l, 4l)
//    val result: List[Long] = new ArrayList[Long]
//    val a: Result.ResultVisitor = new class null {
//      def visit(row: Result.ResultRow): Boolean = {
//        result.add(row.getNumber("a").longValue)
//        return false
//      }
//    }
//    objectUnderTest.accept(a)
//    assertThat(result, containsInAnyOrder(1l))
//  }
//
//  @Test
//  @throws(classOf[Exception])
//  def no_unnecessary_object_creation {
//    val objectUnderTest: ExecutionResult = createInnerExecutionResult("a", 1l, 2l)
//    val result: Set[Integer] = new HashSet[Integer]
//    val a: Result.ResultVisitor = new class null {
//      def visit(row: Result.ResultRow): Boolean = {
//        result.add(row.hashCode)
//        return true
//      }
//    }
//    objectUnderTest.accept(a)
//    assertThat(result, hasSize(1))
//  }
//
//  @Test
//  @throws(classOf[Exception])
//  def no_outofbounds_on_empty_result {
//    val objectUnderTest: ExecutionResult = createInnerExecutionResult("a")
//    val a: Result.ResultVisitor = new class null {
//      def visit(row: Result.ResultRow): Boolean = {
//        fail("the visit should never be called on empty result")
//        return true
//      }
//    }
//    objectUnderTest.accept(a)
//  }

}
