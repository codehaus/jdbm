/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot.
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2000 (C) Cees de Groot. All Rights Reserved.
 * Copyright 2000-2001 (C) Alex Boisvert. All Rights Reserved.
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 * $Id: CacheRecordManager.java,v 1.3 2002/10/13 18:36:35 boisvert Exp $
 */

package jdbm.recman;

import jdbm.RecordManager;
import jdbm.helper.CacheEvictionException;
import jdbm.helper.CachePolicy;
import jdbm.helper.WrappedRuntimeException;
import java.io.IOException;

/**
 *  A RecordManager wrapping and caching another RecordManager.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @author <a href="cg@cdegroot.com">Cees de Groot</a>
 * @version $Id: CacheRecordManager.java,v 1.3 2002/10/13 18:36:35 boisvert Exp $
 */
public class CacheRecordManager
    implements RecordManager
{

    /**
     * Wrapped RecordManager
     */
    protected RecordManager _recman;


    /**
     * Cache for underlying RecordManager
     */
    protected CachePolicy _cache;


    /**
     * Construct a CacheRecordManager wrapping another RecordManager and
     * using a given cache policy.
     *
     * @param recman Wrapped RecordManager
     * @param cache Cache policy
     */
    public CacheRecordManager( RecordManager recman, CachePolicy cache )
    {
        if ( recman == null ) {
            throw new IllegalArgumentException( "Argument 'recman' is null" );
        }
        if ( cache == null ) {
            throw new IllegalArgumentException( "Argument 'cache' is null" );
        }
        _recman = recman;
        _cache = cache;
    }


    /**
     *  Inserts a new record.
     *
     *  @param data the data for the new record.
     *  @returns the rowid for the new record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public long insert( byte[] data )
        throws IOException
    {
        checkIfClosed();

        long recid = _recman.insert( data );
        try {
            _cache.put( new Long( recid ), data );
        } catch ( CacheEvictionException except ) {
            throw new WrappedRuntimeException( except );
        }
        return recid;
    }


    /**
     * Get the underlying Record Manager.
     *
     * @return underlying RecordManager or null if CacheRecordManager has
     *         been closed. 
     */
    public RecordManager getRecordManager()
    {
        return _recman;
    }

    
    /**
     * Get the underlying cache policy
     *
     * @return underlying CachePolicy or null if CacheRecordManager has
     *         been closed. 
     */
    public CachePolicy getCachePolicy()
    {
        return _cache;
    }

    
    /**
     *  Inserts a new record.  This is a utility method which serializes the
     *  object into a byte[].
     *
     *  @param obj the object for the new record.
     *  @returns the rowid for the new record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public long insert( Object obj )
        throws IOException
    {
        checkIfClosed();

        long recid = _recman.insert( obj );
        try {
            _cache.put( new Long( recid ), obj );
        } catch ( CacheEvictionException except ) {
            throw new WrappedRuntimeException( except );
        }
        return recid;
    }


    /**
     *  Deletes a record.
     *
     *  @param rowid the rowid for the record that should be deleted.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public void delete( long recid )
        throws IOException
    {
        checkIfClosed();

        _recman.delete( recid );
        _cache.remove( new Long( recid ) );
    }


    /**
     *  Updates a record.
     *
     *  @param recid the recid for the record that is to be updated.
     *  @param data the new data for the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public void update( long recid, byte[] data )
        throws IOException
    {
        checkIfClosed();

        _recman.update( recid, data );
        try {
            _cache.put( new Long( recid ), data );
        } catch ( CacheEvictionException except ) {
            throw new WrappedRuntimeException( except );
        }
    }


    /**
     *  Updates a record.  This is a utility method which serializes the
     *  object into a byte[].
     *
     *  @param recid the recid for the record that is to be updated.
     *  @param obj the new object for the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public void update( long recid, Object obj )
        throws IOException
    {
        checkIfClosed();

        _recman.update( recid, obj );
        try {
            _cache.put( new Long( recid ), obj );
        } catch ( CacheEvictionException except ) {
            throw new WrappedRuntimeException( except );
        }
    }


    /**
     *  Fetches a record.
     *
     *  @param recid the recid for the record that must be fetched.
     *  @returns the data representing the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public byte[] fetchByteArray( long recid )
        throws IOException
    {
        checkIfClosed();

        byte[] data = (byte[]) _cache.get( new Long( recid ) );
        if ( data == null ) {
            data = _recman.fetchByteArray( recid );
        }
        return data;
    }


    /**
     *  Fetches a record.
     *
     *  @param recid the recid for the record that must be fetched.
     *  @returns the object contained in the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public Object fetchObject( long recid )
        throws IOException, ClassNotFoundException
    {
        checkIfClosed();

        Object obj = _cache.get( new Long( recid ) );
        if ( obj == null ) {
            obj = _recman.fetchObject( recid );
        }
        return obj;
    }


    /**
     *  Closes the record manager.
     *
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public void close()
        throws IOException
    {
        checkIfClosed();

        _recman.close();
        _recman = null;
        _cache = null;
    }


    /**
     *  Returns the number of slots available for "root" rowids. These slots
     *  can be used to store special rowids, like rowids that point to
     *  other rowids. Root rowids are useful for bootstrapping access to
     *  a set of data.
     */
    public int getRootCount()
    {
        checkIfClosed();

        return _recman.getRootCount();
    }


    /**
     *  Returns the indicated root rowid.
     *
     *  @see getRootCount
     */
    public long getRoot( int id )
        throws IOException
    {
        checkIfClosed();

        return _recman.getRoot( id );
    }


    /**
     *  Sets the indicated root rowid.
     *
     *  @see getRootCount
     */
    public void setRoot( int id, long rowid )
        throws IOException
    {
        checkIfClosed();

        _recman.setRoot( id, rowid );
    }


    /**
     * Commit (make persistent) all changes since beginning of transaction.
     */
    public void commit()
        throws IOException
    {
        checkIfClosed();

        _recman.commit();
    }


    /**
     * Rollback (cancel) all changes since beginning of transaction.
     */
    public void rollback()
        throws IOException
    {
        checkIfClosed();

        _recman.rollback();

        // discard all cache entries since we don't know which entries
        // where part of the transaction
        _cache.removeAll();
    }


    /**
     * Obtain the record id of a named object. Returns 0 if named object
     * doesn't exist.
     */
    public long getNamedObject( String name )
        throws IOException
    {
        checkIfClosed();

        return _recman.getNamedObject( name );
    }


    /**
     * Set the record id of a named object.
     */
    public void setNamedObject( String name, long recid )
        throws IOException
    {
        checkIfClosed();

        _recman.setNamedObject( name, recid );
    }


    /**
     * Check if RecordManager has been closed.  If so, throw an
     * IllegalStateException
     */
    private void checkIfClosed()
        throws IllegalStateException
    {
        if ( _recman == null ) {
            throw new IllegalStateException( "RecordManager has been closed" );
        }
    }

}
