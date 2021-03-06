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
package org.neo4j.bolt.v1.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.bolt.transport.BoltProtocol;
import org.neo4j.bolt.v1.messaging.MessageFormat;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.PackStreamMessageFormatV1;
import org.neo4j.bolt.v1.messaging.msgprocess.TransportBridge;
import org.neo4j.bolt.v1.runtime.Session;
import org.neo4j.bolt.v1.runtime.internal.Neo4jError;
import org.neo4j.kernel.impl.logging.LogService;

/**
 * Implements version one of the Bolt Protocol when transported over a socket. This means this class will handle a
 * simple message framing protocol and forward messages to the messaging protocol implementation, version 1.
 * <p/>
 * Versions of the framing protocol are lock-step with the messaging protocol versioning.
 */
public class BoltProtocolV1 implements BoltProtocol
{
    public static final int VERSION = 1;

    private static final int DEFAULT_OUTPUT_BUFFER_SIZE = 8192;

    private final ChunkedOutput chunkedOutput;
    private final MessageFormat.Writer packer;
    private final BoltV1Dechunker dechunker;

    private final Session session;

    private final AtomicInteger inFlight = new AtomicInteger( 0 );
    private final TransportBridge bridge;

    public BoltProtocolV1( Session session, Channel outputChannel, LogService logging )
    {
        // TODO; this part of the Bolt server side is rather messy - notably, the MessageHandler, Session and Session.Callback interfaces all
        //       should reasonably be able to be refactored into something much less complicated.
        //       Likewise the tracking of when to flush the outbound channel - if we moved that logic to ThreadedSessions, a lot of the complexity
        //       below could likely be undone.
        this.chunkedOutput = new ChunkedOutput( outputChannel, DEFAULT_OUTPUT_BUFFER_SIZE );
        this.packer = new PackStreamMessageFormatV1.Writer( new Neo4jPack.Packer( chunkedOutput ), chunkedOutput );
        this.session = session;
        this.bridge = new TransportBridge( logging.getInternalLog( getClass() ), session, packer, this::onMessageDone );
        this.dechunker = new BoltV1Dechunker( bridge, this::onMessageStarted );
    }

    /**
     * Handle an incoming network packet. We currently deal with the chunked input by building up full messages in
     * RAM before we deserialize them. This is fine with most messages, but will become a problem with very large
     * parameters and so on. The next step will be to write a new protocol V1 deserializer that can do incremental
     * deserialization, see the Netty HTTP parser for an example.
     */
    @Override
    public void handle( ChannelHandlerContext channelContext, ByteBuf data ) throws IOException
    {
        try
        {
            dechunker.handle( data );
        }
        catch ( Throwable e )
        {
            bridge.handleFatalError( Neo4jError.from( e ) );

            // close input, keep output open. we still need to write error back to the client higher in the
            // call stack. we are not going to read anything after the error.
            closeInput();
        }
        finally
        {
            data.release();
        }
    }

    @Override
    public int version()
    {
        return VERSION;
    }

    @Override
    public synchronized void close()
    {
        closeInput();
        closeOutput();
    }

    private void closeInput()
    {
        dechunker.close();
        session.close();
    }

    private void closeOutput()
    {
        chunkedOutput.close();
    }

    /*
     * Ths methods below are used to track in-flight messages (messages the client has sent us that are waiting to be processed). We use this information
     * to determine when to explicitly flush our output buffers - if there are no more pending messages when a message is done processing, we should flush
     * the buffers for the session.
     */
    private void onMessageStarted()
    {
        inFlight.incrementAndGet();
    }

    // Note: This will get called from another thread; specifically, while most of the code in this class runs in an IO Thread, this method gets
    //       called from a the session worker thread. This smells bad, and can likely be resolved by moving this whole "when to flush" logic to something
    //       that hooks into ThreadedSessions somehow, since that class has a lot of knowledge about when there are no pending requests.
    private void onMessageDone()
    {
        // If this is the last in-flight message, and we're not in the middle of reading another message over the wire
        // If we are in the middle of a message, we assume there's no need for us to flush partial outbound buffers, we simply
        // wait for more stuff to do to fill the buffers up in order to use network buffers maximally.
        if ( inFlight.decrementAndGet() == 0 && !dechunker.isInMiddleOfAMessage() )
        {
            try
            {
                // Then flush outbound buffers
                packer.flush();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}
