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

package jdbm.helper;

import junit.framework.*;
import java.io.IOException;
import jdbm.recman.RecordManager;
import jdbm.recman.TestRecordFile;

/**
 *  Unit test for {@link MRU}.
 *
 *  @author <a href="mailto:boisvert@exoffice.com>Alex Boisvert</a>
 *  @version $Id: TestObjectCache.java,v 1.1 2000/05/24 23:22:10 boisvert Exp $
 */
public class TestObjectCache extends TestCase {

    public TestObjectCache(String name) {
        super(name);
    }
    
    public void setUp() {
        TestRecordFile.deleteTestFile();
    }

    public void tearDown() {
        TestRecordFile.deleteTestFile();
    }

    /**
     * Test constructor
     */
    public void testConstructor() throws IOException {

        RecordManager recman = new RecordManager(TestRecordFile.testFileName);
        MRU mru = new MRU(1);
        ObjectCache oc = new ObjectCache(recman, mru);

    }
    
    /**
     *  Test that cache is correctly synchronized with RecordManager
     */
    public void testSynchronize() throws IOException, ClassNotFoundException {

        RecordManager recman = new RecordManager(TestRecordFile.testFileName);
        MRU mru = new MRU(1);
        ObjectCache oc = new ObjectCache(recman, mru);

        long recid = recman.insert("test1");

        oc.update(recid, "test2");

        assertEquals("test2", recman.fetchObject(recid));
        assertEquals("test2", oc.fetchObject(recid));
    }

    /**
     *  Test dirty object synchronization
     */
    public void testDirtyFlush() throws IOException, ClassNotFoundException {

        RecordManager recman = new RecordManager(TestRecordFile.testFileName);
        MRU mru = new MRU(1);
        ObjectCache oc = new ObjectCache(recman, mru);

        long recid1 = recman.insert("test1");
        long recid2 = recman.insert("test2");

        oc.update(recid1, "test1a");
        oc.update(recid2, "test2a");

        recman.close();
        oc.dispose();

        recman = new RecordManager(TestRecordFile.testFileName);
        mru = new MRU(1);
        oc = new ObjectCache(recman, mru);

        assertEquals("test1a", oc.fetchObject(recid1));
        assertEquals("test2a", oc.fetchObject(recid2));
    }

    /**
     *  Test dirty object synchronization
     */
    public void testCache() throws IOException, ClassNotFoundException {

        RecordManager recman = new RecordManager(TestRecordFile.testFileName);
        MRU mru = new MRU(3);
        ObjectCache oc = new ObjectCache(recman, mru);

        long recid1 = recman.insert("test1");
        long recid2 = recman.insert("test2");
        long recid3 = recman.insert("test3");
        long recid4 = recman.insert("test4");
        long recid5 = recman.insert("test5");

        oc.update(recid1, "test1a");
        oc.update(recid2, "test2a");
        oc.update(recid3, "test3a");
        oc.update(recid4, "test4a");
        oc.update(recid5, "test5a");

        assertEquals("test1a", oc.fetchObject(recid1));
        assertEquals("test2a", oc.fetchObject(recid2));
        assertEquals("test3a", oc.fetchObject(recid3));
        assertEquals("test4a", oc.fetchObject(recid4));
        assertEquals("test5a", oc.fetchObject(recid5));
    }

    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(new TestSuite(TestObjectCache.class));
    }
}
