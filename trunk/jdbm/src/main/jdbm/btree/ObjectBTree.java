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

import jdbm.RecordManager;

import jdbm.helper.ObjectBAComparator;
import jdbm.helper.Serialization;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;
import jdbm.helper.WrappedRuntimeException;

import java.io.IOException;

import java.util.Comparator;

/**
 * Wrapper for a B+Tree data structure, which allows manipulating
 * serializable objects rather than byte arrays.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: ObjectBTree.java,v 1.1 2002/05/31 06:33:20 boisvert Exp $
 */
public final class ObjectBTree
{
    /**
     * Wrapped BTree
     */
    private BTree _tree;


    /**
     * Construct an ObjectBTree wrapping a BTree.
     */
    private ObjectBTree( BTree tree )
    {
        _tree = tree;
    }


    /**
     * Create a new persistent BTree, with 16 entries per node.
     *
     * @param recman Record manager used for persistence.
     * @param comparator Comparator used to order index entries
     */
    public static ObjectBTree createInstance( RecordManager recman,
                                              Comparator comparator )
        throws IOException
    {
        BTree tree;

        comparator = new ObjectBAComparator( comparator );
        tree = BTree.createInstance( recman, comparator );
        return new ObjectBTree( tree );
    }


    /**
     * Create a new persistent BTree with the given number of entries per node.
     *
     * @param recman Record manager used for persistence.
     * @param comparator Comparator used to order index entries
     * @param pageSize Number of entries per page (must be even).
     */
    public static ObjectBTree createInstance( RecordManager recman,
                                              Comparator comparator,
                                              int pageSize )
        throws IOException
    {
        BTree tree;

        comparator = new ObjectBAComparator( comparator );
        tree = BTree.createInstance( recman, comparator, pageSize );
        return new ObjectBTree( tree );
    }


    /**
     * Load a persistent BTree.
     *
     * @arg recman RecordManager used to store the persistent btree
     * @arg recid Record id of the BTree
     */
    public static ObjectBTree load( RecordManager recman, long recid )
        throws IOException
    {
        BTree tree;

        tree = BTree.load( recman, recid );
        return new ObjectBTree( tree );
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
    public Object insert( Object key, Object value, boolean replace )
        throws IOException
    {
        Object existing;

        if ( key == null ) {
            throw new IllegalArgumentException( "Argument 'key' is null" );
        }
        if ( value == null ) {
            throw new IllegalArgumentException( "Argument 'value' is null" );
        }

        existing = _tree.insert( Serialization.serialize( key ),
                                 Serialization.serialize( value ),
                                 replace );

        if ( existing != null ) {
            try {
                existing = Serialization.deserialize( (byte[]) existing );
            } catch ( ClassNotFoundException except ) {
                throw new jdbm.helper.WrappedRuntimeException( except );
            }
        }

        return existing;
    }


    /**
     * Remove an entry with the given key from the BTree.
     *
     * @param key Removal key
     * @return Value associated with the key, or null if no entry with given
     *         key existed in the BTree.
     */
    public Object remove( Object key )
        throws IOException
    {
        Object existing;

        if ( key == null ) {
            throw new IllegalArgumentException( "Argument 'key' is null" );
        }

        existing = _tree.remove( Serialization.serialize( key ) );
        if ( existing != null ) {
            try {
                existing = Serialization.deserialize( (byte[]) existing );
            } catch ( ClassNotFoundException except ) {
                throw new jdbm.helper.WrappedRuntimeException( except );
            }
        }

        return existing;
    }


    /**
     * Find the value associated with the given key.
     *
     * @param key Lookup key.
     * @return Value associated with the key, or null if not found.
     */
    public Object find( Object key )
        throws IOException
    {
        if ( key == null ) {
            throw new IllegalArgumentException( "Argument 'key' is null" );
        }

        byte[] bytes = _tree.find( Serialization.serialize( key ) );
        if ( bytes == null ) {
            return null;
        } else {
            try {
                return Serialization.deserialize( bytes );
            } catch ( ClassNotFoundException except ) {
                throw new jdbm.helper.WrappedRuntimeException( except );
            }
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
    public Tuple findGreaterOrEqual( Object key )
        throws IOException
    {
        Tuple   tuple;
        byte[]  value;

        if ( key == null ) {
            // there can't be a key greater than or equal to "null"
            // because null is considered an infinite key.
            return null;
        }

        tuple = _tree.findGreaterOrEqual( Serialization.serialize( key ) );

        if ( tuple != null ) {
            value = (byte[]) tuple.getValue();
            try {
                tuple.setValue( Serialization.deserialize( value ) );
            } catch ( ClassNotFoundException except ) {
                throw new WrappedRuntimeException( except );
            }
        }
        return tuple;
    }


    /**
     * Get a browser initially positioned at the beginning of the BTree.
     *
     * @return Browser positionned at the beginning of the BTree.
     */
    public synchronized TupleBrowser browse()
        throws IOException
    {
        TupleBrowser browser;

        browser = _tree.browse();

        return new BrowserDeserializer( browser );
    }


    /**
     * Get a browser initially positioned just before the given key.
     *
     * @param key Key used to position the browser.  If null, the browser
     *            will be positionned after the last entry of the BTree.
     *            (Null is considered to be an "infinite" key)
     * @return Browser positionned just before the given key.
     */
    public TupleBrowser browse( Object key )
        throws IOException
    {
        TupleBrowser browser;

        browser = _tree.browse( Serialization.serialize( key ) );

        return new BrowserDeserializer( browser );
    }


    /**
     * Return the number of entries (size) of the BTree.
     */
    public int size()
    {
        return _tree.size();
    }


    /**
     * Return the persistent record identifier of the BTree.
     */
    public long getRecid()
    {
        return _tree.getRecid();
    }


    /** INNER CLASS
     *
     * A browser wrapper which converts byte arrays back into objects.
     */
    static class BrowserDeserializer
        extends TupleBrowser
    {

        /**
         * Underlying browser
         */
        private TupleBrowser _browser;


        BrowserDeserializer( TupleBrowser browser )
        {
            _browser = browser;
        }


        public boolean getNext( Tuple tuple )
            throws IOException
        {
            byte[] value;

            if ( _browser.getNext( tuple ) ) {
                // convert byte array into object
                value = (byte[]) tuple.getValue();
                try {
                    tuple.setValue( Serialization.deserialize( value ) );
                } catch ( ClassNotFoundException except ) {
                    throw new WrappedRuntimeException( except );
                }
                return true;
            }

            return false;
        }


        public boolean getPrevious( Tuple tuple )
            throws IOException
        {
            byte[] value;

            if ( _browser.getPrevious( tuple ) ) {
                // convert byte array into object
                value = (byte[]) tuple.getValue();
                try {
                    tuple.setValue( Serialization.deserialize( value ) );
                } catch ( ClassNotFoundException except ) {
                    throw new WrappedRuntimeException( except );
                }
                return true;
            }

            return false;
        }

    } // class BrowserDeserializer

}
