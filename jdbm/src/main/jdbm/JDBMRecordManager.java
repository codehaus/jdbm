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
 * Contributions are (C) Copyright 2000 by their associated contributors.
 *
 * $Id: JDBMRecordManager.java,v 1.2 2000/05/24 01:52:11 boisvert Exp $
 */

package jdbm;

import jdbm.recman.RecordManager;
import jdbm.hash.HTree;
import jdbm.helper.ObjectCache;
import jdbm.helper.MRU;
import java.io.IOException;

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
 */
public final class JDBMRecordManager {
    /**
     * Default number of objects kept in cache
     */
    private static final int MAX_OBJECT_CACHE = 100;


    /** Transactional record manager.  This class is really
     *  just a wrapper around it, providing implicit transaction
     *  demarcation for every operations.
     */
    private RecordManager _recman;

    /**
     * Object cache to reduce serialization/deserialization of objects
     */
    private ObjectCache _cache;

    /**
     *  Creates a record manager for the indicated file
     *
     *  @throws IOException when the file cannot be opened or is not
     *          a valid file content-wise.
     */
    public JDBMRecordManager(String filename) throws IOException {
        _recman = new RecordManager(filename);
        _cache = new ObjectCache(_recman, new MRU(MAX_OBJECT_CACHE));
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
    public synchronized void disableTransactions() {
        _recman.disableTransactions();
    }
    

    /**
     *  Closes the record manager.
     *
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void close() throws IOException {
        _recman.close();
        _cache.dispose();
    }
    
    /**
     *  Inserts a new record.
     *
     *  @param data the data for the new record.
     *  @returns the rowid for the new record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized long insert(byte[] data) throws IOException {
        long id = _recman.insert(data);
        _recman.commit();
        return id;
    }


    /**
     *  Inserts a new record.
     *
     *  @param obj the object for the new record.
     *  @returns the rowid for the new record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized long insert(Object obj) throws IOException {
        long id = _recman.insert(obj);
        _recman.commit();
        return id;
    }

    
    /**
     *  Deletes a record.
     *
     *  @param rowid the rowid for the record that should be deleted.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void delete(long recid) throws IOException {
        _recman.delete(recid);
        _recman.commit();
    }


    /**
     *  Updates a record.
     *
     *  @param recid the recid for the record that is to be updated.
     *  @param data the new data for the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void update(long recid, byte[] data)
    throws IOException {
        _recman.update(recid, data);
        _recman.commit();
    }


    /**
     *  Updates a record.
     *
     *  @param recid the recid for the record that is to be updated.
     *  @param obj the new object for the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void update(long recid, Object obj)
    throws IOException {
        _cache.update(recid, obj);
        _recman.commit();
    }

    /**
     *  Fetches a record.
     *
     *  @param recid the recid for the record that must be fetched.
     *  @returns the data representing the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized byte[] fetchByteArray(long recid) throws IOException {
        byte[] data = _recman.fetchByteArray(recid);
        _recman.commit();
        return data;
    }

    /**
     *  Fetches a record.
     *
     *  @param recid the recid for the record that must be fetched.
     *  @returns the object representing the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized Object fetchObject(long recid) 
    throws IOException, ClassNotFoundException {
        Object obj = _cache.fetchObject(recid);
        _recman.commit();
        return obj;
    }

    /**
     *  Returns the number of slots available for "root" rowids. These slots
     *  can be used to store special rowids, like rowids that point to
     *  other rowids. Root rowids are useful for bootstrapping access to
     *  a set of data.
     */
    public int getRootCount() {
        return _recman.getRootCount();
    }

    /**
     *  Returns the indicated root rowid.
     *
     *  @see getRootCount
     */
    public synchronized long getRoot(int id) throws IOException {
        long rid = _recman.getRoot(id);
        _recman.commit();
        return rid;
    }

    /**
     *  Sets the indicated root rowid.
     *
     *  @see getRootCount
     */
    public synchronized void setRoot(int id, long rowid) throws IOException {
        int invalidRoot = RecordManager.NAME_DIRECTORY_ROOT;
        if (id == invalidRoot) {
            throw new IllegalArgumentException("Root "+invalidRoot+
                                               " is reserved for"+
                                               " named objects directory");
        }
        _recman.setRoot(id, rowid);
        _recman.commit();
    }

    /**
     * Obtain a named persistent hashtable.
     */
    public synchronized JDBMHashtable getHashtable(String name) 
    throws IOException {
        long root_recid = _recman.getNamedObject(name);
        if (root_recid == 0) {
            // create a new one
            root_recid = HTree.createRootDirectory(_recman);
            _recman.setNamedObject(name, root_recid);
            _recman.commit();
        }
        return new HTreeWrapper(_recman, _cache, root_recid);
    }
}

/**
 * Wrapper around HTree so that we have implicit transaction commit for
 * every JDBMHashtable operation.
 */
class HTreeWrapper implements JDBMHashtable {

    private RecordManager _recman;
    private HTree _tree;


    HTreeWrapper(RecordManager recman, ObjectCache cache, long root_recid) 
    throws IOException {
        _recman = recman;
        _tree = new HTree(recman, cache, root_recid);
    }

    /**
     * Associates the specified value with the specified key.
     *
     * @arg key key with which the specified value is to be assocated.
     * @arg value value to be associated with the specified key.
     */
    public void put(Object key, Object value) throws IOException {
        _tree.put(key, value);
        _recman.commit();
    }

    /**
     * Returns the value which is associated with the given key. Returns
     * <code>null</code> if there is not association for this key.
     *
     * @arg key key whose associated value is to be returned
     */
    public Object get(Object key) throws IOException {
        Object obj = _tree.get(key);
        _recman.commit();
        return obj;
    }

    /**
     * Remove the value which is associated with the given key.  If the
     * key does not exist, this method simply ignores the operation.
     *
     * @arg key key whose associated value is to be removed
     */
    public void remove(Object key) throws IOException {
        _tree.remove(key);
        _recman.commit();
    }

    /**
     * Returns an enumeration of the keys contained in this hashtable.
     */
    public JDBMEnumeration keys() throws IOException {
        return _tree.keys();
    }


    /**
     * Returns an enumeration of the values contained in this hashtable.
     */
    public JDBMEnumeration values() throws IOException {
        return _tree.values();
    }

    /**
     * Disposes from this hashtable instance and frees associated
     * resources.
     */
    public void dispose() throws IOException {
        _tree.dispose();
        _tree = null;
        _recman = null;
    }

}
