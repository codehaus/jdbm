/*
 *  $Id: TestTransactionManager.java,v 1.1 2000/05/06 00:00:53 boisvert Exp $
 *
 *  Unit tests for TransactionManager class
 *
 *  Simple db toolkit
 *  Copyright (C) 1999, 2000 Cees de Groot <cg@cdegroot.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Library General Public License 
 *  as published by the Free Software Foundation; either version 2 
 *  of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Library General Public License for more details.
 *
 *  You should have received a copy of the GNU Library General Public License 
 *  along with this library; if not, write to the Free Software Foundation, 
 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 */
package jdbm.recman;

import junit.framework.*;
import java.io.*;

/**
 *  This class contains all Unit tests for {@link TransactionManager}.
 */
public class TestTransactionManager extends TestCase {

    public TestTransactionManager(String name) {
  super(name);
    }
    
    public void setUp() {
  TestRecordFile.deleteTestFile();
    }
    public void tearDown() {
    }

    /**
     *  Test constructor. Oops - can only be done indirectly :-)
     */
    public void testCtor() throws Exception {
  RecordFile file = new RecordFile(TestRecordFile.testFileName);
    }

    /**
     *  Test recovery
     */
    public void testRecovery() throws Exception {
  RecordFile file = new RecordFile(TestRecordFile.testFileName);

  // Do three transactions.
  for (int i = 0; i < 3; i++) {
      BlockIo node = file.get(i);
      node.setDirty();
      file.release(node);
      file.commit();
  }
  assertDataSizeEquals("len1", 0);
  assertLogSizeNotZero("len1");
  
  // Leave the old record file in flux, and open it again.
  // The second instance should start recovery.
  file = new RecordFile(TestRecordFile.testFileName);
  file.close();
  assertDataSizeEquals("len2", 3 * RecordFile.BLOCK_SIZE);
  assertLogSizeEquals("len2", 8);

  // assure we can recover this log file
  file = new RecordFile(TestRecordFile.testFileName);
    }
    
    /**
     *  Test background synching
     */
    public void testSynching() throws Exception {
  RecordFile file = new RecordFile(TestRecordFile.testFileName);

  // Do enough transactions to fill the first slot
  int txnCount = TransactionManager.TXNS_IN_LOG + 5;
  for (int i = 0; i < txnCount; i++) {
      BlockIo node = file.get(i);
      node.setDirty();
      file.release(node);
      file.commit();
  }

  // The data file now has the first slotfull
  assertDataSizeEquals("len1", TransactionManager.TXNS_IN_LOG *
           RecordFile.BLOCK_SIZE);
  assertLogSizeNotZero("len1");
  
  // Leave the old record file in flux, and open it again.
  // The second instance should start recovery.
  file = new RecordFile(TestRecordFile.testFileName);
  file.close();

  assertDataSizeEquals("len2", txnCount * RecordFile.BLOCK_SIZE);
  assertLogSizeEquals("len2", 8);
    }

    //  Helpers

    void assertDataSizeEquals(String msg, long size) {
  assertEquals(msg + " data size", size,
         new File(TestRecordFile.testFileName 
            + RecordFile.extension).length());
    }
    void assertLogSizeEquals(String msg, long size) {
  assertEquals(msg + " log size", size,
         new File(TestRecordFile.testFileName 
            + TransactionManager.extension).length());
    }
    void assertLogSizeNotZero(String msg) {
  assert(msg + " log size",
         new File(TestRecordFile.testFileName 
      + TransactionManager.extension).length() != 0);
    }
    
    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
  junit.textui.TestRunner.run(new TestSuite(TestTransactionManager.class));
    }
}
