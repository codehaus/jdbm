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

package jdbm.htree;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.recman.TestRecordFile;
import jdbm.helper.FastIterator;
import junit.framework.*;
import java.io.IOException;
import java.util.Properties;

/**
 *  This class contains all Unit tests for {@link HTree}.
 *
 *  @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 *  @version $Id$
 */
public class TestHTree extends TestCase {

    public TestHTree(String name) {
        super(name);
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
    public void testIterator() throws IOException {
        Properties props = new Properties();
        RecordManager recman = RecordManagerFactory.createRecordManager( TestRecordFile.testFileName, props );

        HTree testTree = getHtree(recman, "htree");
    
        int total = 10;
        for ( int i = 0; i < total; i++ ) {
            testTree.put( Long.valueOf("" + i), Long.valueOf("" + i) );
        }
        recman.commit();
    
        FastIterator fi = testTree.values();
        Object item;
        int count = 0;
        while( (item = fi.next()) != null ) {
            count++;
        }
        assertEquals( count, total );

        recman.close();
    }


    private static HTree getHtree( RecordManager recman, String name )
      throws IOException
    {
        long recId = recman.getNamedObject("htree");  
        HTree testTree;
        if ( recId != 0 ) {
            testTree = HTree.load( recman, recId );
        } else {
            testTree = HTree.createInstance( recman );
            recman.setNamedObject( "htree", testTree.getRecid() );
        }
        return testTree;
    }


    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(new TestSuite(TestHTree.class));
    }

}
