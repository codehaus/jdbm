/*
 *  $Id: TestRecordFile.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
 *
 *  Unit tests for RecordFile class
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
 *  This class contains all Unit tests for {@link RecordFile}.
 */
public class TestRecordFile extends TestCase {

    final static String testFileName = "test";

    public TestRecordFile(String name) {
	super(name);
    }
    
    static void deleteTestFile() {
	try {
	    new File(testFileName + RecordFile.extension).delete();
	} catch (Exception e) {	}
	try {
	    new File(testFileName + TransactionManager.extension).delete();
	} catch (Exception e) {
	}
    }
    
    public void setUp() {
	deleteTestFile();
    }
    
    public void tearDown() {
	deleteTestFile();
    }
    

    /**
     *  Test constructor
     */
    public void testCtor() throws Exception {
	RecordFile f = new RecordFile(testFileName);
    }

    /**
     *  Test addition of record 0
     */
    public void testAddZero() throws Exception {
	RecordFile f = new RecordFile(testFileName);
	byte[] data = f.get(0).getData();
	data[14] = (byte) 'b';
	f.release(0, true);
	f.close();
	f = new RecordFile(testFileName);
	data = f.get(0).getData();
	assertEquals((byte) 'b', data[14]);
	f.release(0, false);
	f.close();
    }
    
    /**
     *  Test addition of a number of records, with holes.
     */
    public void testWithHoles() throws Exception {
	RecordFile f = new RecordFile(testFileName);

	// Write recid 0, byte 0 with 'b'
	byte[] data = f.get(0).getData();
	data[0] = (byte) 'b';
	f.release(0, true);

	// Write recid 10, byte 10 with 'c'
	data = f.get(10).getData();
	data[10] = (byte) 'c';
	f.release(10, true);

	// Write recid 5, byte 5 with 'e' but don't mark as dirty
	data = f.get(5).getData();
	data[5] = (byte) 'e';
	f.release(5, false);

	f.close();

	f = new RecordFile(testFileName);
	data = f.get(0).getData();
	assertEquals("0 = b", (byte) 'b', data[0]);
	f.release(0, false);
	
	data = f.get(5).getData();
	assertEquals("5 = 0", 0, data[5]);
	f.release(5, false);
    
	data = f.get(10).getData();
	assertEquals("10 = c", (byte) 'c', data[10]);
	f.release(10, false);

	f.close();
    }

    /**
     *  Test wrong release
     */
    public void testWrongRelease() throws Exception {
	RecordFile f = new RecordFile(testFileName);

	// Write recid 0, byte 0 with 'b'
	byte[] data = f.get(0).getData();
	data[0] = (byte) 'b';
	try {
	    f.release(1, true);
	    fail("expected exception");
	} catch (IOException e) {
	}
	f.release(0, false);
	
	f.close();
    }
    

    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
	junit.textui.TestRunner.run(new TestSuite(TestRecordFile.class));
    }
}
