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
 * Copyright 2001 (C) Alex Boisvert. All Rights Reserved.
 * Contributions are Copyright (C) 2001 by their associated contributors.
 *
 */

package jdbm.btree;

import jdbm.recman.RecordManager;

import jdbm.helper.Comparator;
import jdbm.helper.ObjectCache;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * B+Tree persistent indexing data structure.  B+Trees are optimized for
 * block-based, random I/O storage because they store multiple keys on
 * one tree node (called <code>BPage</code>).  In addition, the leaf nodes
 * directly contain (inline) the values associated with the keys, allowing a
 * single (or sequential) disk read of all the values on the page.
 * <p>
 * B+Trees are n-airy, yeilding log(N) search cost.  They are self-balancing,
 * preventing search performance degradation when the size of the tree grows.
 * <p>
 * Keys and associated values must be <code>Serializable</code> objects. The
 * user is responsible to supply a serializable <code>Comparator</code> object
 * to be used for the ordering of entries, which are also called <code>Tuple</code>.
 * The B+Tree allows traversing the keys in forward and reverse order using a
 * TupleBrowser obtained from the browse() methods.
 * <p>
 * This implementation does not directly support duplicate keys, but it is
 * possible to handle duplicates by inlining or referencing an object collection
 * as a value.
 * <p>
 * There is no limit on key size or value size, but it is recommended to keep
 * both as small as possible to reduce disk I/O.   This is especially true for
 * the key size, which impacts all non-leaf <code>BPage</code> objects.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: BTree.java,v 1.3 2001/11/10 19:52:38 boisvert Exp $
 */
public class BTree implements Externalizable {

    private static final boolean DEBUG = false;

    /**
     * Version id for serialization.
     */
    final static long serialVersionUID = 1L;


    /**
     * Default page size (number of entries per node)
     */
    public static final int DEFAULT_SIZE = 16;


    /**
     * Page manager used to persist changes in BPages
     */
    protected transient RecordManager _recman;


    /**
     * Object cache to reduce serializing/deserializing objects
     */
    protected transient ObjectCache _cache;


    /**
     * This BTree's record ID in the PageManager.
     */
    private transient long _recid;


    /**
     * Comparator used to index entries.
     */
    protected Comparator _comparator;


    /**
     * Height of the B+Tree.  This is the number of BPages you have to traverse
     * to get to a leaf BPage, starting from the root.
     */
    private int _height;


    /**
     * Recid of the root BPage
     */
    private transient long _root;


    /**
     * Number of entries in each BPage.
     */
    private int _pageSize;


    /**
     * Total number of entries in the BTree
     */
    protected int _size;


    /**
     * No-argument constructor used by serialization.
     */
    public BTree() {
        // empty
    }


    /**
     * Create a new persistent BTree, with 16 entries per node.
     *
     * @param recman Record manager used for persistence.
     * @param cache Object cache for the record manager
     * @param comparator Comparator used to order index entries
     */
    public BTree( RecordManager recman, ObjectCache cache,
                  Comparator comparator )
    throws IOException {
        this( recman, cache, comparator, DEFAULT_SIZE );
    }


    /**
     * Create a new persistent BTree with the given number of entries per node.
     *
     * @param recman Record manager used for persistence.
     * @param cache Object cache for the record manager
     * @param comparator Comparator used to order index entries
     * @param pageSize Number of entries per page (must be even).
     */
    public BTree( RecordManager recman, ObjectCache cache,
                  Comparator comparator, int pageSize )
    throws IOException {

        if ( recman == null ) {
            throw new IllegalArgumentException( "Argument 'recman' is null" );
        }

        if ( cache == null ) {
            throw new IllegalArgumentException( "Argument 'cache' is null" );
        }

        if ( comparator == null ) {
            throw new IllegalArgumentException( "Argument 'comparator' is null" );
        }

        // make sure there's an even number of
        if ( ( pageSize & 1 ) != 0 ) {
            throw new IllegalArgumentException( "Argument 'pageSize' must be even");
        }

        _comparator = comparator;
        _pageSize = pageSize;
        _recman = recman;
        _cache = cache;

        _recid = _recman.insert( this );
    }


    /**
     * Load a persistent BTree.
     *
     * @arg recman RecordManager used to store the persistent btree
     * @arg cache Cache for the record manager.
     * @arg recid Record id of the BTree
     */
    public static BTree load( RecordManager recman, ObjectCache cache,
                              long recid )
    throws IOException {
        try {
            BTree btree = (BTree) recman.fetchObject( recid );
            btree._recid = recid;
            btree._recman = recman;
            btree._cache = cache;
            return btree;
        } catch ( ClassNotFoundException except ) {
            throw new Error( except.getMessage() );
        }
    }


    /**
     * Insert an entry in the BTree.
     * <p>
     * The BTree cannot store duplicate entries.  An existing entry can be
     * replaced using the <code>replace</code> flag.   If an entry with the
     * same key already exists in the BTree, its value is returned.
     *
     * @param key Insert key
     * @param value Insert value
     * @param replace Set to true to replace an existing key-value pair.
     * @return Existing value, if any.
     */
    public synchronized Object insert( Object key, Object value, boolean replace )
    throws IOException {
        if ( key == null ) {
            throw new IllegalArgumentException( "Argument 'key' is null" );
        }
        if ( value == null ) {
            throw new IllegalArgumentException( "Argument 'value' is null" );
        }

        BPage rootPage = getRoot();

        if ( rootPage == null ) {
            // BTree is currently empty, create a new root BPage
            if (DEBUG) {
                System.out.println( "BTree.insert() new root BPage" );
            }
            rootPage = new BPage( this, key, value, _pageSize );
            _root = rootPage._recid;
            _height = 1;
            _size = 1;
            _cache.update( _recid, this );
            return null;
        } else {
            BPage.InsertResult insert = rootPage.insert( _height, key, value, replace );
            boolean dirty = false;
            if ( insert._overflow != null ) {
                // current root page overflowed, we replace with a new root page
                if ( DEBUG ) {
                    System.out.println( "BTree.insert() replace root BPage due to overflow" );
                }
                rootPage = new BPage( this, rootPage, insert._overflow, _pageSize );
                _root = rootPage._recid;
                _height += 1;
                dirty = true;
            }
            if ( insert._existing == null ) {
                _size++;
                dirty = true;
            }
            if ( dirty ) {
                _cache.update( _recid, this );
            }
            // insert might have returned an existing value
            return insert._existing;
        }
    }


    /**
     * Remove an entry with the given key from the BTree.
     *
     * @param key Removal key
     * @return Value associated with the key, or null if no entry with given
     *         key existed in the BTree.
     */
    public synchronized Object remove( Object key )
    throws IOException {
        if ( key == null ) {
            throw new IllegalArgumentException( "Argument 'key' is null" );
        }

        BPage rootPage = getRoot();
        if ( rootPage == null ) {
            return null;
        }
        boolean dirty = false;
        BPage.RemoveResult remove = rootPage.remove( _height, key );
        if ( remove._underflow && rootPage.isEmpty() ) {
            _height -= 1;
            dirty = true;

            // TODO:  check contract for BPages to be removed from recman.
            if ( _height == 0 ) {
                _root = 0;
            } else {
                _root = rootPage.childBPage( _pageSize-1 )._recid;
            }
        }
        if ( remove._value != null ) {
            _size--;
            dirty = true;
        }
        if ( dirty ) {
            _cache.update( _recid, this );
        }
        return remove._value;
    }


    /**
     * Find the value associated with the given key.
     *
     * @param key Lookup key.
     * @return Value associated with the key, or null if not found.
     */
    public synchronized Object find( Object key ) throws IOException {
        if ( key == null ) {
            throw new IllegalArgumentException( "Argument 'key' is null" );
        }
        BPage rootPage = getRoot();
        if ( rootPage == null ) {
            return null;
        }

        Tuple tuple = new Tuple( null, null );
        TupleBrowser browser = rootPage.find( _height, key );

        if ( browser.getNext( tuple ) ) {
            // find returns the matching key or the next ordered key, so we must
            // check if we have an exact match
            if ( _comparator.compare( key, tuple.getKey() ) != 0 ) {
                return null;
            } else {
                return tuple.getValue();
            }
        } else {
            return null;
        }
    }


    /**
     * Find the value associated with the given key, or the entry immediately
     * following this key in the ordered BTree.
     *
     * @param key Lookup key.
     * @return Value associated with the key, or a greater entry, or null if no
     *         greater entry was found.
     */
    public synchronized Tuple findGreaterOrEqual(Object key)
    throws IOException {
        Tuple tuple = new Tuple( null, null );
        TupleBrowser browser = browse( key );
        if ( browser.getNext( tuple ) ) {
            return tuple;
        } else {
            return null;
        }
    }


    /**
     * Get a browser initially positioned at the beginning of the BTree.
     * <p><b>
     * WARNING:  If you make structural modifications to the BTree during
     * browsing, you will get inconsistent browing results.
     * </b>
     *
     * @return Browser positionned at the beginning of the BTree.
     */
    public synchronized TupleBrowser browse()
    throws IOException {
        BPage rootPage = getRoot();
        if ( rootPage == null ) {
            return EmptyBrowser.instance;
        }
        TupleBrowser browser = rootPage.findFirst();
        return browser;
    }


    /**
     * Get a browser initially positioned just before the given key.
     * <p><b>
     * WARNING:  If you make structural modifications to the BTree during
     * browsing, you will get inconsistent browing results.
     * </b>
     *
     * @param key Key used to position the browser.  If null, the browser
     *            will be positionned after the last entry of the BTree.
     *            (Null is considered to be an "infinite" key)
     * @return Browser positionned just before the given key.
     */
    public synchronized TupleBrowser browse( Object key )
    throws IOException {
        /*
        if ( key == null ) {
            throw new IllegalArgumentException( "Argument 'key' is null" );
        }
        */
        BPage rootPage = getRoot();
        if ( rootPage == null ) {
            return EmptyBrowser.instance;
        }
        TupleBrowser browser = rootPage.find( _height, key );
        return browser;
    }


    /**
     * Return the number of entries (size) of the BTree.
     */
    public synchronized int size() {
        return _size;
    }


    /**
     * Return the persistent record identifier of the BTree.
     */
    public long getRecid() {
        return _recid;
    }


    /**
     * Return the root BPage, or null if it doesn't exist.
     */
    private BPage getRoot() throws IOException {
        if ( _root == 0 ) {
            return null;
        }
        try {
            BPage root = (BPage) _cache.fetchObject( _root );
            root._recid = _root;
            root._btree = this;
            return root;
        } catch ( ClassNotFoundException except ) {
            throw new Error( except.toString() );
        }
    }

    /**
     * Implement Externalizable interface.
     */
    public void readExternal( ObjectInput in )
    throws IOException, ClassNotFoundException {
        _comparator = (Comparator) in.readObject();
        _height = in.readInt();
        _root = in.readLong();
        _pageSize = in.readInt();
        _size = in.readInt();
    }


    /**
     * Implement Externalizable interface.
     */
    public void writeExternal( ObjectOutput out )
    throws IOException {
        out.writeObject( _comparator );
        out.writeInt( _height );
        out.writeLong( _root );
        out.writeInt( _pageSize );
        out.writeInt( _size );
    }


    /*
    public void assert() throws IOException {
        BPage root = getRoot();
        if ( root != null ) {
            root.assertRecursive( _height );
        }
    }
    */


    /*
    public void dump() throws IOException {
        BPage root = getRoot();
        if ( root != null ) {
            root.dumpRecursive( _height, 0 );
        }
    }
    */


    /** PRIVATE INNER CLASS
     *  Browser returning no element.
     */
    static class EmptyBrowser extends TupleBrowser {

        static TupleBrowser instance = new EmptyBrowser();

        public boolean getNext( Tuple tuple ) {
            return false;
        }

        public boolean getPrevious( Tuple tuple ) {
            return false;
        }
    }
}
