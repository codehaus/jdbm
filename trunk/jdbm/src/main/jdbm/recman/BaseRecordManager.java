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
 * $Id: BaseRecordManager.java,v 1.2 2002/06/14 16:22:38 boisvert Exp $
 */

package jdbm.recman;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import jdbm.RecordManager;

/**
 *  This class manages records, which are uninterpreted blobs of data. The
 *  set of operations is simple and straightforward: you communicate with
 *  the class using long "rowids" and byte[] data blocks. Rowids are returned
 *  on inserts and you can stash them away someplace safe to be able to get
 *  back to them. Data blocks can be as long as you wish, and may have
 *  lengths different from the original when updating.
 *  <p>
 *  Operations are synchronized, so that only one of them will happen
 *  concurrently even if you hammer away from multiple threads. Operations
 *  are made atomic by keeping a transaction log which is recovered after
 *  a crash, so the operations specified by this interface all have ACID
 *  properties.
 *  <p>
 *  You identify a file by just the name. The package attaches <tt>.db</tt>
 *  for the database file, and <tt>.lg</tt> for the transaction log. The
 *  transaction log is synchronized regularly and then restarted, so don't
 *  worry if you see the size going up and down.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @author <a href="cg@cdegroot.com">Cees de Groot</a>
 * @version $Id: BaseRecordManager.java,v 1.2 2002/06/14 16:22:38 boisvert Exp $
 */
public final class BaseRecordManager
    implements RecordManager
{

    /**
     * Underlying record file.
     */
    private RecordFile _file;


    /**
     * Physical row identifier manager.
     */
    private PhysicalRowIdManager _physMgr;


    /**
     * Logigal to Physical row identifier manager.
     */
    private LogicalRowIdManager _logMgr;


    /**
     * Page manager.
     */
    private PageManager _pageman;


    /**
     * Reserved slot for name directory.
     */
    public static final int NAME_DIRECTORY_ROOT = 0;


    /**
     * Directory of named JDBMHashtables.  This directory is a persistent
     * directory, stored as a Hashtable.  It can be retrived by using
     * the NAME_DIRECTORY_ROOT.
     */
    private Map _nameDirectory;


    /**
     *  Creates a record manager for the indicated file
     *
     *  @throws IOException when the file cannot be opened or is not
     *          a valid file content-wise.
     */
    public BaseRecordManager( String filename )
        throws IOException
    {
        _file = new RecordFile( filename );
        _pageman = new PageManager( _file );
        _physMgr = new PhysicalRowIdManager( _file, _pageman );
        _logMgr = new LogicalRowIdManager( _file, _pageman );
    }


    /**
     *  Switches off transactioning for the record manager. This means
     *  that a) a transaction log is not kept, and b) writes aren't
     *  synch'ed after every update. This is useful when batch inserting
     *  into a new database.
     *  <p>
     *  Only call this method directly after opening the file, otherwise
     *  the results will be undefined.
     */
    public synchronized void disableTransactions()
    {
        checkIfClosed();

        _file.disableTransactions();
    }


    /**
     *  Closes the record manager.
     *
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void close()
        throws IOException
    {
        checkIfClosed();

        _pageman.close();
        _pageman = null;

        _file.close();
        _file = null;
    }


    /**
     *  Inserts a new record.
     *
     *  @param data the data for the new record.
     *  @returns the rowid for the new record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized long insert( byte[] data )
        throws IOException
    {
        checkIfClosed();

        Location physRowId = _physMgr.insert( data );
        return _logMgr.insert( physRowId ).toLong();
    }


    /**
     *  Inserts a new record.
     *
     *  @param obj the object for the new record.
     *  @returns the rowid for the new record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public long insert( Object obj )
        throws IOException
    {
        checkIfClosed();

        byte[] buffer = objectToByteArray( obj );
        return insert( buffer );
    }

    /**
     *  Deletes a record.
     *
     *  @param rowid the rowid for the record that should be deleted.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void delete( long recid )
        throws IOException
    {
        checkIfClosed();

        Location logRowId = new Location( recid );
        Location physRowId = _logMgr.fetch( logRowId );
        _physMgr.delete( physRowId );
        _logMgr.delete( logRowId );
    }


    /**
     *  Updates a record.
     *
     *  @param recid the recid for the record that is to be updated.
     *  @param data the new data for the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void update( long recid, byte[] data )
        throws IOException
    {
        checkIfClosed();

        Location logRecid = new Location( recid );
        Location physRecid = _logMgr.fetch( logRecid );
        Location newRecid = _physMgr.update( physRecid, data );
        if ( ! newRecid.equals( physRecid ) ) {
            _logMgr.update( logRecid, newRecid );
        }
    }


    /**
     *  Updates a record.
     *
     *  @param recid the recid for the record that is to be updated.
     *  @param obj the new object for the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public void update( long recid, Object obj )
        throws IOException
    {
        checkIfClosed();

        byte[] buffer = objectToByteArray( obj );
        update( recid, buffer );
    }


    /**
     *  Fetches a record.
     *
     *  @param recid the recid for the record that must be fetched.
     *  @returns the data representing the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized byte[] fetchByteArray( long recid )
        throws IOException
    {
        checkIfClosed();
        if ( recid <= 0 ) {
            throw new IllegalArgumentException( "Argument 'recid' is invalid: "
                                                + recid );
        }

        return _physMgr.fetch( _logMgr.fetch( new Location( recid ) ) );
    }


    /**
     *  Fetches a record.
     *
     *  @param recid the recid for the record that must be fetched.
     *  @returns the object contained in the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized Object fetchObject( long recid )
        throws IOException, ClassNotFoundException
    {
        byte[]       buffer;

        checkIfClosed();
        if ( recid <= 0 ) {
            throw new IllegalArgumentException( "Argument 'recid' is invalid: "
                                                + recid );
        }
        buffer = _physMgr.fetch( _logMgr.fetch( new Location( recid ) ) );
        return byteArrayToObject( buffer );
    }


    /**
     *  Returns the number of slots available for "root" rowids. These slots
     *  can be used to store special rowids, like rowids that point to
     *  other rowids. Root rowids are useful for bootstrapping access to
     *  a set of data.
     */
    public int getRootCount()
    {
        return FileHeader.NROOTS;
    }

    /**
     *  Returns the indicated root rowid.
     *
     *  @see getRootCount
     */
    public synchronized long getRoot( int id )
        throws IOException
    {
        checkIfClosed();

        return _pageman.getFileHeader().getRoot( id );
    }


    /**
     *  Sets the indicated root rowid.
     *
     *  @see getRootCount
     */
    public synchronized void setRoot( int id, long rowid )
        throws IOException
    {
        checkIfClosed();

        _pageman.getFileHeader().setRoot( id, rowid );
    }


    /**
     * Obtain the record id of a named object. Returns 0 if named object
     * doesn't exist.
     */
    public long getNamedObject( String name )
        throws IOException
    {
        checkIfClosed();

        Map nameDirectory = getNameDirectory();
        Long recid = (Long) nameDirectory.get( name );
        if ( recid == null ) {
            return 0;
        }
        return recid.longValue();
    }

    /**
     * Set the record id of a named object.
     */
    public void setNamedObject( String name, long recid )
        throws IOException
    {
        checkIfClosed();

        Map nameDirectory = getNameDirectory();
        if ( recid == 0 ) {
            // remove from hashtable
            nameDirectory.remove( name );
        } else {
            nameDirectory.put( name, new Long( recid ) );
        }
        saveNameDirectory( nameDirectory );
    }


    /**
     * Commit (make persistent) all changes since beginning of transaction.
     */
    public void commit()
        throws IOException
    {
        checkIfClosed();

        _pageman.commit();
    }


    /**
     * Rollback (cancel) all changes since beginning of transaction.
     */
    public void rollback()
        throws IOException
    {
        checkIfClosed();

        _pageman.rollback();
    }


    /**
     * Recreate a serialized object from a byte array.
     */
    private static Object byteArrayToObject( byte[] array )
        throws IOException, ClassNotFoundException
    {
        ByteArrayInputStream bais = new ByteArrayInputStream( array );
        ObjectInputStream ois = new ObjectInputStream( bais );
        return ois.readObject();
    }


    /**
     * Serialize an object into a byte array.
     */
    private static byte[] objectToByteArray( Object obj )
        throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream( baos );
        oos.writeObject( obj );
        oos.close();
        baos.close();
        return baos.toByteArray();
    }


    /**
     * Load name directory
     */
    private Map getNameDirectory()
        throws IOException
    {
        // retrieve directory of named hashtable
        long nameDirectory_recid = getRoot( NAME_DIRECTORY_ROOT );
        if ( nameDirectory_recid == 0 ) {
            _nameDirectory = new HashMap();
            nameDirectory_recid = insert( _nameDirectory );
            setRoot( NAME_DIRECTORY_ROOT, nameDirectory_recid );
        } else {
            try {
                _nameDirectory = (Map) fetchObject( nameDirectory_recid );
            } catch ( ClassNotFoundException cnfe ) {
                cnfe.printStackTrace();
                throw new Error( "NAME_DIRECTORY_ROOT " +
                                 "must point to a Map" );
            }
        }
        return _nameDirectory;
    }


    private void saveNameDirectory( Map directory )
        throws IOException
    {
        long recid = getRoot( NAME_DIRECTORY_ROOT );
        if ( recid == 0 ) {
            throw new IOException( "Name directory must exist" );
        }
        update( recid, _nameDirectory );
    }


    /**
     * Check if RecordManager has been closed.  If so, throw an
     * IllegalStateException.
     */
    private void checkIfClosed()
        throws IllegalStateException
    {
        if ( _file == null ) {
            throw new IllegalStateException( "RecordManager has been closed" );
        }
    }
}
