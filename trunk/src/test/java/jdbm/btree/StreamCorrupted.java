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


import jdbm.RecordManager;

import jdbm.RecordManagerFactory;

import jdbm.helper.StringComparator;

import jdbm.recman.TestRecordFile;

import jdbm.btree.BTree;


import java.io.IOException;


import junit.framework.*;


/**
 * Contributed test case for BTree by Christof Dallermassl (cdaller@iicm.edu):
 * <p/>
 * <p/>
 * <p/>
 * -= quote from original message posted on jdbm-general =-
 * <p/>
 * <pre>
 * <p/>
 * <p/>
 * <p/>
 * I tried to insert a couple of elements into a BTree and then remove
 * <p/>
 * them one by one. After a number or removals, there is always (if more
 * <p/>
 * than 20 elements in btree) a java.io.StreamCorruptedException thrown.
 * <p/>
 * <p/>
 * <p/>
 * The strange thing is, that on 50 elements, the exception is thrown
 * <p/>
 * after removing 22, on 200 it is thrown after 36, on 1000 it is thrown
 * <p/>
 * after 104, on 10000 it is thrown after 1003....
 * <p/>
 * <p/>
 * <p/>
 * The full stackTrace is here:
 * <p/>
 * ---------------------- snip ------- snap -------------------------
 * <p/>
 * java.io.StreamCorruptedException: Caught EOFException while reading the
 * <p/>
 * stream header
 * <p/>
 *   at java.io.ObjectInputStream.readStreamHeader(ObjectInputStream.java:845)
 * <p/>
 *   at java.io.ObjectInputStream.<init>(ObjectInputStream.java:168)
 * <p/>
 *   at jdbm.recman.RecordManager.byteArrayToObject(RecordManager.java:296)
 * <p/>
 *   at jdbm.recman.RecordManager.fetchObject(RecordManager.java:239)
 * <p/>
 *   at jdbm.helper.ObjectCache.fetchObject(ObjectCache.java:104)
 * <p/>
 *   at jdbm.btree.BPage.loadBPage(BPage.java:670)
 * <p/>
 *   at jdbm.btree.BPage.remove(BPage.java:492)
 * <p/>
 *   at jdbm.btree.BPage.remove(BPage.java:437)
 * <p/>
 *   at jdbm.btree.BTree.remove(BTree.java:313)
 * <p/>
 *   at JDBMTest.main(JDBMTest.java:41)
 * <p/>
 * <p/>
 * <p/>
 * </pre>
 *
 * @author <a href="mailto:cdaller@iicm.edu">Christof Dallermassl</a>
 * @version $Id$
 */

public class StreamCorrupted

    extends TestCase

{


    public StreamCorrupted( String name )
    {

        super( name );

    }


    public void setUp()
    {

        TestRecordFile.deleteTestFile();

    }


    public void tearDown()
    {

        TestRecordFile.deleteTestFile();

    }


    /**
     * Basic tests
     */

    public void testStreamCorrupted()

        throws IOException

    {

        RecordManager recman;

        BTree btree;

        int iterations;


        iterations = 100; // 23 works :-(((((

        // open database

        recman = RecordManagerFactory.createRecordManager( TestRecordFile.testFileName );

        // create a new B+Tree data structure

        btree = BTree.createInstance( recman, new StringComparator() );

        recman.setNamedObject( "testbtree", btree.getRecid() );

        // action:

        // insert data

        for ( int count = 0; count < iterations; count++ )
        {

            btree.insert( "num" + count, new Integer( count ), true );

        }

        // delete data

        for ( int count = 0; count < iterations; count++ )
        {

            btree.remove( "num" + count );

        }

        // close database

        recman.close();

        recman = null;

    }


    /**
     * Runs all tests in this class
     */

    public static void main( String[] args )
    {

        junit.textui.TestRunner.run( new TestSuite( StreamCorrupted.class ) );

    }


}

