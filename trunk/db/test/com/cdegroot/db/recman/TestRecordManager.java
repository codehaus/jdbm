/*
 *  $Id: TestRecordManager.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
 *
 *  Unit tests for RecordManager class
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
package com.cdegroot.db.recman;

import junit.framework.*;
import java.io.File;
import java.io.IOException;

/**
 *  This class contains all Unit tests for {@link RecordManager}.
 */
public class TestRecordManager extends TestCase {

    public TestRecordManager(String name) {
	super(name);
    }
    
    public void setUp() {
	TestRecordFile.deleteTestFile();
    }
    public void tearDown() {
	//TestRecordFile.deleteTestFile();
    }

    /**
     *  Test constructor
     */
    public void testCtor() throws Exception {
	RecordManager mgr = new RecordManager(TestRecordFile.testFileName);
    }

    /**
     *  Test basics
     */
    public void testBasics() throws Exception {
	RecordManager mgr = new RecordManager(TestRecordFile.testFileName);

	// insert a 10,000 byte record.
	byte[] data = TestUtil.makeRecord(10000, (byte) 1);
	long rowid = mgr.insert(data);
	assert("check data1", 
	       TestUtil.checkRecord(mgr.fetch(rowid), 10000, (byte) 1));
	
	// update it as a 20,000 byte record.
	data = TestUtil.makeRecord(20000, (byte) 2);
	mgr.update(rowid, data);
	assert("check data2", 
	       TestUtil.checkRecord(mgr.fetch(rowid), 20000, (byte) 2));
	
	// insert a third record.
	data = TestUtil.makeRecord(20, (byte) 3);
	long rowid2 = mgr.insert(data);
	assert("check data3", 
	       TestUtil.checkRecord(mgr.fetch(rowid2), 20, (byte) 3));
	
	// now, grow the first record again
	data = TestUtil.makeRecord(30000, (byte) 4);
	mgr.update(rowid, data);
	assert("check data4", 
	       TestUtil.checkRecord(mgr.fetch(rowid), 30000, (byte) 4));
	

	// delete the record
	mgr.delete(rowid);

	// close the file
	mgr.close();
    }

    /**
     *  Test delete and immediate reuse. This attempts to reproduce
     *  a bug in the stress test involving 0 record lengths.
     */
    public void testDeleteAndReuse() throws Exception {
	RecordManager mgr = new RecordManager(TestRecordFile.testFileName);

	// insert a 1500 byte record.
	byte[] data = TestUtil.makeRecord(1500, (byte) 1);
	long rowid = mgr.insert(data);
	assert("check data1", 
	       TestUtil.checkRecord(mgr.fetch(rowid), 1500, (byte) 1));
	

	// delete the record
	mgr.delete(rowid);

	// insert a 0 byte record. Should have the same rowid.
	data = TestUtil.makeRecord(0, (byte) 2);
	long rowid2 = mgr.insert(data);
	assertEquals("old and new rowid", rowid, rowid2);
	assert("check data2", 
	       TestUtil.checkRecord(mgr.fetch(rowid2), 0, (byte) 2));

	// now make the record a bit bigger
	data = TestUtil.makeRecord(10000, (byte) 3);
	mgr.update(rowid, data);
	assert("check data3", 
	       TestUtil.checkRecord(mgr.fetch(rowid), 10000, (byte) 3));
	
	// .. and again
	data = TestUtil.makeRecord(30000, (byte) 4);
	mgr.update(rowid, data);
	assert("check data3", 
	       TestUtil.checkRecord(mgr.fetch(rowid), 30000, (byte) 4));
	
	// close the file
	mgr.close();
    }

    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
	junit.textui.TestRunner.run(new TestSuite(TestRecordManager.class));
    }
}
