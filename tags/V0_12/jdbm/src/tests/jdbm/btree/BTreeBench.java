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
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 */

package jdbm.btree;

import jdbm.helper.LongComparator;
import jdbm.recman.RecordManager;
import jdbm.helper.MRU;
import jdbm.helper.ObjectCache;

import java.util.Enumeration;
import java.util.Hashtable;

import java.io.IOException;

/**
 *  Random insertion/removal test for B+Tree data structure.
 *
 *  @author <a href="mailto:boisvert@exoffice.com">Alex Boisvert</a>
 *  @version $Id: BTreeBench.java,v 1.1 2001/05/19 14:38:19 boisvert Exp $
 */
public class BTreeBench {

    static final int ITERATIONS = 1000000;

    public static void main( String[] args ) {

        RecordManager recman;
        ObjectCache cache;
        BTree tree = null;

        try {
            recman = new RecordManager( "test" );
            cache = new ObjectCache( recman, new MRU( 2000 ) );
            tree = new BTree( recman, cache, new LongComparator(), 32 );

            Hashtable hash = new Hashtable();

            for ( int i=0; i<ITERATIONS; i++) {
                Long random = new Long( random( 0, 64000 ) );

                if ( ( i % 5000 ) == 0 ) {
                    System.out.println( "Iterations=" + i + " Objects=" + tree.size() );
                    recman.commit();
                }
                if ( hash.get( random ) == null ) {
                    //System.out.println( "Insert " + random );
                    hash.put( random, random );
                    tree.insert( random, random, false );
                } else {
                    //System.out.println( "Remove " + random );
                    hash.remove( random );
                    Object removed = (Object) tree.remove( random );
                    if ( ( removed == null ) || ( ! removed.equals( random ) ) ) {
                        throw new IllegalStateException( "Remove expected " + random + " got " + removed );
                    }
                }
                // tree.assert();
                compare( tree, hash );
            }

            recman.close();
        } catch ( Throwable except ) {
            except.printStackTrace();
        }
    }

    static long random( int min, int max ) {
        return Math.round( Math.random() * ( max-min) ) + min;
    }

    static void compare( BTree tree, Hashtable hash ) throws IOException {
        boolean failed = false;
        Enumeration enum;

        if ( tree.size() != hash.size() ) {
            throw new IllegalStateException( "Tree size " + tree.size() + " Hash size " + hash.size() );
        }

        enum = hash.keys();
        while ( enum.hasMoreElements() ) {
            Object key = (Object) enum.nextElement();
            Object hashValue = hash.get( key );
            Object treeValue = tree.find( key );
            if ( ! hashValue.equals( treeValue ) ) {
                System.out.println( "Compare expected " + hashValue + " got " + treeValue );
                failed = true;
            }
        }
        if ( failed ) {
            throw new IllegalStateException( "Compare failed" );
        }
    }

}
