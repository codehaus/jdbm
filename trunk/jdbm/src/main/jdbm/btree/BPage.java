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

import jdbm.helper.Conversion;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Page of a Btree.
 * <p>
 * The page contains a number of key-value pairs.  Keys are ordered to allow
 * dichotomic search.
 * <p>
 * If the page is a leaf page, the keys and values are user-defined and
 * represent entries inserted by the user.
 * <p>
 * If the page is non-leaf, each key represents the greatest key in the
 * underlying BPages and the values are recids pointing to the children BPages.
 * The only exception is the rightmost BPage, which is considered to have an
 * "infinite" key value, meaning that any insert will be to the left of this
 * pseudo-key
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: BPage.java,v 1.3 2002/05/31 06:33:20 boisvert Exp $
 */
public final class BPage
    implements Externalizable
{

    private static final boolean DEBUG = false;


    /**
     * Version id for serialization.
     */
    final static long serialVersionUID = 1L;


    /**
     * Parent B+Tree.
     */
    transient BTree _btree;


    /**
     * This BPage's record ID in the PageManager.
     */
    protected transient long _recid;


    /**
     * Flag indicating if this is a leaf BPage.
     */
    protected boolean _isLeaf;


    /**
     * Keys of children nodes
     */
    protected Object[] _keys;


    /**
     * Values associated with keys.  In case this is a non-leaf BPage, values
     * are recids (java.lang.Long) of children BPages.
     */
    protected Object[] _values;


    /**
     * Index of first used item at the page
     */
    protected int _first;


    /**
     * Previous leaf BPage (only if this BPage is a leaf)
     */
    protected long _previous;


    /**
     * Next leaf BPage (only if this BPage is a leaf)
     */
    protected long _next;



    /**
     * No-argument constructor used by serialization.
     */
    public BPage()
    {
        // empty
    }


    /**
     * Root page overflow constructor
     */
    BPage( BTree btree, BPage root, BPage overflow, int pageSize )
        throws IOException
    {
        _btree = btree;

        _isLeaf = false;

        _first = pageSize-2;

        _keys = new Object[ pageSize ];
        _keys[ pageSize-2 ] = overflow.getLargestKey();
        _keys[ pageSize-1 ] = root.getLargestKey();

        _values = new Object[ pageSize ];
        _values[ pageSize-2 ] = convertRecid( overflow._recid );
        _values[ pageSize-1 ] = convertRecid( root._recid );

        _recid = _btree._recman.insert( this );
    }


    /**
     * Root page (first insert) constructor.
     */
    BPage( BTree btree, byte[] key, byte[] value, int pageSize )
        throws IOException
    {
        _btree = btree;

        _isLeaf = true;

        _first = pageSize-2;

        _keys = new Object[ pageSize ];
        _keys[ pageSize-2 ] = key;
        _keys[ pageSize-1 ] = null;  // I am the root BPage for now

        _values = new Object[ pageSize ];
        _values[ pageSize-2 ] = value;
        _values[ pageSize-1 ] = null;  // I am the root BPage for now

        _recid = _btree._recman.insert( this );
    }


    /**
     * Overflow page constructor.  Creates an empty BPage.
     */
    BPage( BTree btree, int pageSize, boolean isLeaf )
        throws IOException
    {
        _btree = btree;

        _isLeaf = isLeaf;

        // page will initially be half-full
        _first = pageSize/2;

        _keys = new Object[ pageSize ];
        _values = new Object[ pageSize ];

        _recid = _btree._recman.insert( this );
    }


    /**
     * Get largest key under this BPage.  Null is considered to be the
     * greatest possible key.
     */
    byte[] getLargestKey()
    {
        return (byte[]) _keys[ _keys.length-1 ];
    }


    /**
     * Return true if BPage is empty.
     */
    boolean isEmpty()
    {
        return ( _first == _values.length-1 );
    }


    /**
     * Return true if BPage is full.
     */
    boolean isFull() {
        return ( _first == 0 );
    }


    /**
     * Find the object associated with the given key.
     *
     * @param height Height of the current BPage (zero is leaf page)
     * @param key The key
     * @return TupleBrowser positionned just before the given key, or before
     *                      next greater key if key isn't found.
     */
    TupleBrowser find( int height, byte[] key )
        throws IOException
    {
        int index = findChildren( key );

        /*
        if ( DEBUG ) {
            System.out.println( "BPage.find() current: " + this
                                + " height: " + height);
        }
        */

        height -= 1;

        if ( height == 0 ) {
            // leaf BPage
            return new Browser( this, index );
        } else {
            // non-leaf BPage
            BPage child = childBPage( index );
            return child.find( height, key );
        }
    }


    /**
     * Find first entry and return a browser positioned before it.
     *
     * @return TupleBrowser positionned just before the first entry.
     */
    TupleBrowser findFirst()
        throws IOException
    {
        if ( _isLeaf ) {
            return new Browser( this, _first );
        } else {
            BPage child = childBPage( _first );
            return child.findFirst();
        }
    }


    /**
     * Insert the given key and value.
     * <p>
     * Since the Btree does not support duplicate entries, the caller must
     * specify whether to replace the existing value.
     *
     * @param height Height of the current BPage (zero is leaf page)
     * @param key Insert key
     * @param value Insert value
     * @param replace Set to true to replace the existing value, if one exists.
     * @return Insertion result containing existing value OR a BPage if the key
     *         was inserted and provoked a BPage overflow.
     */
    InsertResult insert( int height, byte[] key, byte[] value, boolean replace )
        throws IOException
    {
        InsertResult result;

        int index = findChildren( key );

        height -= 1;
        if ( height == 0 )  {

            result = new InsertResult();

            // inserting on a leaf BPage
            if ( DEBUG ) {
                System.out.println( "Bpage.insert() Insert on leaf Bpage key=" + key
                                    + " value=" + value + " index="+index);
            }
            if ( compare( key, (byte[]) _keys[ index ] ) == 0 ) {
                // key already exists
                if ( DEBUG ) {
                    System.out.println( "Bpage.insert() Key already exists." ) ;
                }
                result._existing = (byte[]) _values[ index ];
                if ( replace ) {
                    _values [ index ] = value;
                    _btree._recman.update( _recid, this );
                }
                // return the existing key
                return result;
            }
        } else {
            // non-leaf BPage
            BPage child = childBPage( index );
            result = child.insert( height, key, value, replace );

            if ( result._existing != null ) {
                // return existing key, if any.
                return result;
            }

            if ( result._overflow == null ) {
                // no overflow means we're done with insertion
                return result;
            }

            // there was an overflow, we need to insert the overflow page
            // on this BPage
            if ( DEBUG ) {
                System.out.println( "BPage.insert() Overflow page: " + result._overflow._recid );
            }
            key = result._overflow.getLargestKey();
            value = convertRecid( result._overflow._recid );

            // update child's largest key
            _keys[ index ] = child.getLargestKey();

            // clean result so we can reuse it
            result._overflow = null;
        }

        // if we get here, we need to insert a new entry on the BPage
        // before _children[ index ]
        if ( !isFull() ) {
            insertEntry( this, index-1, key, value );
            _btree._recman.update( _recid, this );
            return result;
        }


        // page is full, we must divide the page
        int half = _keys.length >> 1;
        BPage newPage = new BPage( _btree, _keys.length, _isLeaf );
        if ( index < half ) {
            // move lower-half of entries to overflow BPage,
            // including new entry
            if ( DEBUG ) {
                System.out.println( "Bpage.insert() move lower-half of entries to overflow BPage, including new entry." ) ;
            }
            copyEntries( this, 0, newPage, half, index );
            setEntry( newPage, half+index, key, value );
            copyEntries( this, index, newPage, half+index+1, half-index-1 );
        } else {
            // move lower-half of entries to overflow BPage,
            // new entry stays on this BPage
            if ( DEBUG ) {
                System.out.println( "Bpage.insert() move lower-half of entries to overflow BPage. New entry stays" ) ;
            }
            copyEntries( this, 0, newPage, half, half );
            copyEntries( this, half, this, half-1, index-half );
            setEntry( this, index-1, key, value );
        }

        _first = half-1;

        // nullify lower half of entries
        for ( int i=0; i<_first; i++ ) {
            setEntry( this, i, null, null );
        }

        if ( _isLeaf ) {
            // link newly created BPage
            newPage._previous = _previous;
            newPage._next = _recid;
            if ( _previous != 0 ) {
                BPage previous = loadBPage( _previous );
                previous._next = newPage._recid;
                _btree._recman.update( _previous, previous );
            }
            _previous = newPage._recid;
        }

        _btree._recman.update( _recid, this );
        _btree._recman.update( newPage._recid, newPage );

        result._overflow = newPage;
        return result;
    }


    /**
     * Remove the entry associated with the given key.
     *
     * @param height Height of the current BPage (zero is leaf page)
     * @param key Removal key
     * @return Remove result object
     */
    RemoveResult remove( int height, byte[] key )
        throws IOException
    {
        RemoveResult result;

        int half = _keys.length / 2;
        int index = findChildren( key );

        height -= 1;
        if ( height == 0 ) {
            // remove leaf entry
            if ( compare( (byte[]) _keys[ index ], key ) != 0 ) {
                throw new IllegalArgumentException( "Key not found: " + key );
            }
            result = new RemoveResult();
            result._value = (byte[]) _values[ index ];
            removeEntry( this, index );

            // update this BPage
            _btree._recman.update( _recid, this );

        } else {
            // recurse into Btree to remove entry on a children page
            BPage child = childBPage( index );
            result = child.remove( height, key );

            // update children
            _keys[ index ] = child.getLargestKey();
            _btree._recman.update( _recid, this );

            if ( result._underflow ) {
                // underflow occured
                if ( child._first != half+1 ) {
                    throw new IllegalStateException( "Error during underflow [1]" );
                }
                if ( index < _values.length-1 ) {
                    // exists greater brother page
                    BPage brother = childBPage( index+1 );
                    int bfirst = brother._first;
                    if ( bfirst < half ) {
                        // steal entries from "brother" page
                        int steal = ( half - bfirst + 1 ) / 2;
                        brother._first += steal;
                        child._first -= steal;
                        copyEntries( child, half+1, child, half+1-steal, half-1 );
                        copyEntries( brother, bfirst, child, 2*half-steal, steal );

                        for ( int i=bfirst; i<bfirst+steal; i++ ) {
                            setEntry( brother, i, null, null );
                        }

                        // update child's largest key
                        _keys[ index ] = child.getLargestKey();

                        // no change in previous/next BPage

                        // update BPages
                        _btree._recman.update( _recid, this );
                        _btree._recman.update( brother._recid, brother );
                        _btree._recman.update( child._recid, child );

                    } else {
                        // move all entries from page "child" to "brother"
                        if ( brother._first != half ) {
                            throw new IllegalStateException( "Error during underflow [2]" );
                        }

                        brother._first = 1;
                        copyEntries( child, half+1, brother, 1, half-1 );
                        _btree._recman.update( brother._recid, brother );

                        // remove "child" from current BPage
                        copyEntries( this, _first, this, _first+1, index-_first );
                        setEntry( this, _first, null, null );
                        _first += 1;
                        _btree._recman.update( _recid, this );

                        // re-link previous and next BPages
                        if ( child._previous != 0 ) {
                            BPage prev = loadBPage( child._previous );
                            prev._next = child._next;
                            _btree._recman.update( prev._recid, prev );
                        }
                        if ( child._next != 0 ) {
                            BPage next = loadBPage( child._next );
                            next._previous = child._previous;
                            _btree._recman.update( next._recid, next );
                        }


                        // delete "child" BPage
                        _btree._recman.delete( child._recid );
                    }
                } else {
                    // page "brother" is before "child"
                    BPage brother = childBPage( index-1 );
                    int bfirst = brother._first;
                    if ( bfirst < half ) {
                        // steal entries from "brother" page
                        int steal = ( half - bfirst + 1 ) / 2;
                        brother._first += steal;
                        child._first -= steal;
                        copyEntries( brother, 2*half-steal, child,
                                     half+1-steal, steal );
                        copyEntries( brother, bfirst, brother,
                                     bfirst+steal, 2*half-bfirst-steal );

                        for ( int i=bfirst; i<bfirst+steal; i++ ) {
                            setEntry( brother, i, null, null );
                        }

                        // update brother's largest key
                        _keys[ index-1 ] = brother.getLargestKey();

                        // no change in previous/next BPage

                        // update BPages
                        _btree._recman.update( _recid, this );
                        _btree._recman.update( brother._recid, brother );
                        _btree._recman.update( child._recid, child );

                    } else {
                        // move all entries from page "brother" to "child"
                        if ( brother._first != half ) {
                            throw new IllegalStateException( "Error during underflow [3]" );
                        }

                        child._first = 1;
                        copyEntries( brother, half, child, 1, half );
                        _btree._recman.update( child._recid, child );

                        // remove "brother" from current BPage
                        copyEntries( this, _first, this, _first+1, index-1-_first );
                        setEntry( this, _first, null, null );
                        _first += 1;
                        _btree._recman.update( _recid, this );

                        // re-link previous and next BPages
                        if ( brother._previous != 0 ) {
                            BPage prev = loadBPage( brother._previous );
                            prev._next = brother._next;
                            _btree._recman.update( prev._recid, prev );
                        }
                        if ( brother._next != 0 ) {
                            BPage next = loadBPage( brother._next );
                            next._previous = brother._previous;
                            _btree._recman.update( next._recid, next );
                        }

                        // delete "brother" BPage
                        _btree._recman.delete( brother._recid );
                    }
                }
            }
        }

        // underflow if page is more than half-empty
        result._underflow = _first > half;

        return result;
    }


    /**
     * Find the first children node with a key equal or greater than the given
     * key.
     *
     * @return index of first children with equal or greater key.
     */
    private int findChildren( byte[] key )
    {
        int left = _first;
        int right = _keys.length-1;

        // binary search
        while ( left < right )  {
            int middle = ( left + right ) / 2;
            if ( compare( (byte[]) _keys[ middle ], key ) < 0 ) {
                left = middle+1;
            } else {
                right = middle;
            }
        }
        return right;
    }


    /**
     * Insert entry at given position.
     */
    private static void insertEntry( BPage page, int index,
                                     byte[] key, byte[] value )
    {
        Object[] keys = page._keys;
        Object[] values = page._values;
        int start = page._first;
        int count = index-page._first+1;

        // shift entries to the left
        System.arraycopy( keys, start, keys, start-1, count );
        System.arraycopy( values, start, values, start-1, count );
        page._first -= 1;
        keys[ index ] = key;
        values[ index ] = value;
    }


    /**
     * Remove entry at given position.
     */
    private static void removeEntry( BPage page, int index )
    {
        Object[] keys = page._keys;
        Object[] values = page._values;
        int start = page._first;
        int count = index-page._first;

        System.arraycopy( keys, start, keys, start+1, count );
        keys[ start ] = null;
        System.arraycopy( values, start, values, start+1, count );
        values[ start ] = null;
        page._first++;
    }


    /**
     * Set the entry at the given index.
     */
    private static void setEntry( BPage page, int index, byte[] key, byte[] value )
    {
        page._keys[ index ] = key;
        page._values[ index ] = value;
    }


    /**
     * Copy entries between two BPages
     */
    private static void copyEntries( BPage source, int indexSource,
                                     BPage dest, int indexDest, int count )
    {
        System.arraycopy( source._keys, indexSource, dest._keys, indexDest, count);
        System.arraycopy( source._values, indexSource, dest._values, indexDest, count);
    }


    /**
     * Return the child BPage at given index.
     */
    BPage childBPage( int index )
        throws IOException
    {
        long recid = convertRecid( (byte[]) _values[ index ] );
        return loadBPage( recid );
    }


    /**
     * Load the BPage at the given recid.
     */
    private BPage loadBPage( long recid )
        throws IOException
    {
        try {
            BPage child = (BPage) _btree._recman.fetchObject( recid );
            child._recid = recid;
            child._btree = _btree;
            return child;
        } catch ( ClassNotFoundException except ) {
            throw new Error( except.toString() );
        }
    }


    /**
     * Convert the given recid into a serializable object.
     */
    private byte[] convertRecid( long recid )
    {
        return Conversion.convertToByteArray( recid );
    }


    /**
     * Convert the given serializable object into a recid.
     */
    private long convertRecid( byte[] recid )
    {
        return Conversion.convertToLong( recid );
    }


    /**
     * Implement Externalizable interface.
     */
    public void readExternal( ObjectInput in )
        throws IOException, ClassNotFoundException
    {
        int size = in.readInt();

        _isLeaf = in.readBoolean();
        if ( _isLeaf ) {
            _previous = in.readLong();
            _next = in.readLong();
        }

        _first = in.readInt();

        _keys = new Object[ size ];
        for ( int i=_first; i<size; i++ ) {
            _keys[ i ] = readByteArray( in );
        }

        _values = new Object[ size ];
        for ( int i=_first; i<size; i++ ) {
            _values[ i ] = readByteArray( in );
        }
    }


    /**
     * Implement Externalizable interface.
     */
    public void writeExternal( ObjectOutput out )
        throws IOException
    {
        out.writeInt( _keys.length );

        out.writeBoolean( _isLeaf );
        if ( _isLeaf ) {
            out.writeLong( _previous );
            out.writeLong( _next );
        }

        out.writeInt( _first );

        for ( int i=_first; i<_keys.length; i++ ) {
            writeByteArray( out, (byte[]) _keys[ i ] );
        }

        for ( int i=_first; i<_keys.length; i++ ) {
            writeByteArray( out, (byte[]) _values[ i ] );
        }
    }

    private final int compare( byte[] value1, byte[] value2 )
    {
        if ( value1 == null ) {
            return 1;
        }
        if ( value2 == null ) {
            return -1;
        }
        return _btree._comparator.compare( value1, value2 );
    }


    static byte[] readByteArray( ObjectInput in )
        throws IOException, ClassNotFoundException
    {
        int len = in.readInt();
        if ( len < 0 ) {
            return null;
        }
        byte[] buf = new byte[ len ];
        in.readFully( buf );
        return buf;
    }


    static void writeByteArray( ObjectOutput out, byte[] buf )
        throws IOException
    {
        if ( buf == null ) {
            out.writeInt( -1 );
        } else {
            out.writeInt( buf.length );
            out.write( buf );
        }
    }


    /**
     * Dump the structure of the tree on the screen.  This is used for debugging
     * purposes only.
     */
    private void dump( int height )
    {
        String prefix = "";
        for ( int i=0; i<height; i++ ) {
           prefix += "    ";
        }
        System.out.println( prefix + "-------------------------------------- BPage recid=" + _recid);
        System.out.println( prefix + "first=" + _first );
        for ( int i=0; i< _keys.length; i++ ) {
            System.out.println( prefix + "Bpage [" + i + "] " + _keys[ i ] + " " + _values[ i ] );
        }
        System.out.println( prefix + "--------------------------------------" );
    }


    /**
     * Recursively dump the state of the BTree on screen.  This is used for
     * debugging purposes only.
     */
    void dumpRecursive( int height, int level )
        throws IOException
    {
        height -= 1;
        level += 1;
        if ( height > 0 ) {
            for ( int i=_first; i<_keys.length; i++ ) {
                if ( _keys[ i ] == null ) break;
                BPage child = childBPage( i );
                child.dump( level );
                child.dumpRecursive( height, level );
            }
        }
    }


    /**
     * Assert the ordering of the keys on the BPage.  This is used for testing
     * purposes only.
     */
    private void assert()
    {
        for ( int i=_first; i<_keys.length-1; i++ ) {
            if ( compare( (byte[]) _keys[ i ], (byte[]) _keys[ i+1 ] ) >= 0 ) {
                dump( 0 );
                throw new Error( "BPage not ordered" );
            }
        }
    }


    /**
     * Recursively assert the ordering of the BPage entries on this page
     * and sub-pages.  This is used for testing purposes only.
     */
    void assertRecursive( int height ) throws IOException {
        assert();
        if ( --height > 0 ) {
            for ( int i=_first; i<_keys.length; i++ ) {
                if ( _keys[ i ] == null ) break;
                BPage child = childBPage( i );
                if ( compare( (byte[]) _keys[ i ], child.getLargestKey() ) != 0 ) {
                    dump( 0 );
                    child.dump( 0 );
                    throw new Error( "Invalid child subordinate key" );
                }
                child.assertRecursive( height );
            }
        }
    }


    /** STATIC INNER CLASS
     *  Result from insert() method call
     */
    static class InsertResult {

        /**
         * Overflow page.
         */
        BPage _overflow;

        /**
         * Existing value for the insertion key.
         */
        byte[] _existing;

    }

    /** STATIC INNER CLASS
     *  Result from remove() method call
     */
    static class RemoveResult {

        /**
         * Set to true if underlying pages underflowed
         */
        boolean _underflow;

        /**
         * Removed entry value
         */
        byte[] _value;
    }


    /** PRIVATE INNER CLASS
     * Browser to traverse leaf BPages.
     */
    static class Browser
        extends TupleBrowser
    {

        /**
         * Current page.
         */
        private BPage _page;


        /**
         * Current index in the page.  The index positionned on the next
         * tuple to return.
         */
        private int _index;


        /**
         * Create a browser.
         *
         * @param page Current page
         * @param index Position of the next tuple to return.
         */
        Browser( BPage page, int index )
        {
            _page = page;
            _index = index;
        }

        public boolean getNext( Tuple tuple )
            throws IOException
        {
            if ( _index < _page._keys.length ) {
                if ( _page._keys[ _index ] == null ) {
                    // reached end of the tree.
                    return false;
                }
            } else if ( _page._next != 0 ) {
                // move to next page
                _page = _page.loadBPage( _page._next );
                _index = _page._first;
            }
            tuple.setKey( _page._keys[ _index ] );
            tuple.setValue( _page._values[ _index ] );
            _index++;
            return true;
        }

        public boolean getPrevious( Tuple tuple )
            throws IOException
        {
            if ( _index == _page._first ) {

                if ( _page._previous != 0 ) {
                    _page = _page.loadBPage( _page._previous );
                    _index = _page._keys.length;
                } else {
                    // reached beginning of the tree
                    return false;
                }
            }
            _index--;
            tuple.setKey( _page._keys[ _index ] );
            tuple.setValue( _page._values[ _index ] );
            return true;

        }
    }

}
