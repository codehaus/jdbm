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

import jdbm.JDBMEnumeration;
import jdbm.recman.RecordManager;
import jdbm.recman.TestRecordFile;
import jdbm.helper.MRU;
import jdbm.helper.ObjectCache;
import jdbm.helper.StringComparator;
import junit.framework.*;
import java.io.IOException;
import java.util.Hashtable;

/**
 *  This class contains all Unit tests for {@link BTree}.
 *
 *  @author <a href="mailto:boisvert@exoffice.com">Alex Boisvert</a>
 *  @version $Id: TestBTree.java,v 1.1 2001/05/19 14:38:19 boisvert Exp $
 */
public class TestBTree extends TestCase {

    static final boolean DEBUG = false;

    public TestBTree( String name ) {
        super( name );
    }

    public void setUp() {
        TestRecordFile.deleteTestFile();
    }

    public void tearDown() {
        TestRecordFile.deleteTestFile();
    }


    /**
     *  Basic tests
     */
    public void testBasics() throws IOException {
        if ( DEBUG ) {
            System.out.println("TestBTree.testBasics");
        }

        RecordManager recman = new RecordManager( "test" );
        ObjectCache cache = new ObjectCache( recman, new MRU( 100 ) );
        BTree tree = new BTree( recman, cache, new StringComparator() );

        tree.insert( "test1", "value1", false);
        tree.insert( "test2", "value2", false);

        String result;
        result = (String) tree.find( "test0" );
        if ( result != null ) {
            throw new Error( "Test0 shouldn't be found" );
        }

        result = (String) tree.find( "test1" );
        if ( result == null || !result.equals( "value1" ) ) {
            throw new Error( "Invalid value for test1: " + result );
        }

        result = (String) tree.find( "test2" );
        if ( result == null || !result.equals( "value2" ) ) {
            throw new Error( "Invalid value for test2: " + result );
        }

        result = (String) tree.find( "test3" );
        if ( result != null ) {
            throw new Error( "Test3 shouldn't be found" );
        }

        recman.close();

    }


    /**
     *  Runs all tests in this class
     */
    public static void main( String[] args ) {
        junit.textui.TestRunner.run( new TestSuite( TestBTree.class ) );
    }

}
