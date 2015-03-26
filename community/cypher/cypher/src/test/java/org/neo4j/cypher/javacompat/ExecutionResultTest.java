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
package org.neo4j.cypher.javacompat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.cypher.ArithmeticException;
import org.neo4j.cypher.ExtendedExecutionResult;
import org.neo4j.cypher.internal.compatibility.ExecutionResultWrapperFor2_3;
import org.neo4j.cypher.internal.compiler.v2_3.PlannerName;
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.query.QueryExecutionMonitor;
import org.neo4j.kernel.impl.query.QuerySession;
import org.neo4j.kernel.impl.util.ResourceIterators;
import org.neo4j.test.TestGraphDatabaseFactory;
import scala.NotImplementedError;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExecutionResultTest
{
    private GraphDatabaseAPI db;
    private ExecutionEngine engine;

    @Before
    public void setUp() throws Exception
    {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .newGraphDatabase();
        engine = new ExecutionEngine( db );
    }

    @Test
    public void shouldCloseTransactionsWhenIteratingResults() throws Exception
    {
        // Given an execution result that has been started but not exhausted
        createNode();
        createNode();
        ExecutionResult executionResult = engine.execute( "MATCH (n) RETURN n" );
        ResourceIterator<Map<String, Object>> resultIterator = executionResult.iterator();
        resultIterator.next();
        assertThat( activeTransaction(), is( notNullValue() ) );

        // When
        resultIterator.close();

        // Then
        assertThat( activeTransaction(), is( nullValue() ) );
    }

    @Test
    public void shouldCloseTransactionsWhenIteratingOverSingleColumn() throws Exception
    {
        // Given an execution result that has been started but not exhausted
        createNode();
        createNode();
        ExecutionResult executionResult = engine.execute( "MATCH (n) RETURN n" );
        ResourceIterator<Node> resultIterator = executionResult.columnAs( "n" );
        resultIterator.next();
        assertThat( activeTransaction(), is( notNullValue() ) );

        // When
        resultIterator.close();

        // Then
        assertThat( activeTransaction(), is( nullValue() ) );
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowAppropriateException() throws Exception
    {
        engine.execute( "RETURN rand()/0" ).iterator().next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowAppropriateExceptionAlsoWhenVisiting() throws Exception
    {
        engine.execute( "RETURN rand()/0" ).accept( new Result.ResultVisitor()
        {
            @Override
            public boolean visit( Result.ResultRow row )
            {
                return true;
            }
        } );
    }


    private void createNode()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
    }

    private org.neo4j.kernel.TopLevelTransaction activeTransaction()
    {
        ThreadToStatementContextBridge bridge = db.getDependencyResolver().resolveDependency(
                ThreadToStatementContextBridge.class );
        return bridge.getTopLevelTransactionBoundToThisThread( false );
    }

    private ExecutionResult createInnerExecutionResult( final String column, final Object... values )
    {
        InternalExecutionResult mock = mock(InternalExecutionResult.class);
        final AtomicInteger offset = new AtomicInteger();
        when(mock.hasNext()).thenReturn(offset.get() < values.length);
        when(mock.next()).thenAnswer(new Answer<Map<String , Object>>() {
            @Override
            public Map<String, Object> answer(InvocationOnMock invocationOnMock) throws Throwable {
                Map<String, Object> result = new HashMap<>();
                result.put( column, values[offset.get()] );
                offset.incrementAndGet();
                return result;
            }
        });
        when(mock.javaIterator()).thenReturn(ResourceIterators.EMPTY_ITERATOR);
        ExtendedExecutionResult inner = new ExecutionResultWrapperFor2_3(mock, mock(PlannerName.class), mock(QueryExecutionMonitor.class), mock(QuerySession.class));
        return new ExecutionResult(inner);
    }
}
