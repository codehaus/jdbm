/*
 *  $Id: TestEntryPage.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
 *
 *  Unit tests for EntryPage class
 *
 *  Simple db toolkit
 *  Copyright (C) 1999, 2000 Cees de Groot <cg@cdegroot.com>
 */
package com.cdegroot.db.hash;

import junit.framework.*;
import java.io.File;
import java.io.IOException;
import com.cdegroot.db.recman.*;

/**
 *  This class contains all Unit tests for {@link EntryPage}.
 */
public class TestEntryPage extends TestCase {

    public void setUp() {
	TestRecordFile.deleteTestFile();
    }
    public void tearDown() {
	TestRecordFile.deleteTestFile();
    }
    

    public TestEntryPage(String name) {
	super(name);
    }

    /**
     *  Test make a root page
     */
    public void testMakeAEntryPage() throws Exception {
	RecordFile f = new RecordFile(TestRecordFile.testFileName);
	PageManager pm = new PageManager(f);

	long rootid = pm.allocate(Magic.HASH_ROOT_PAGE);
	BlockIo rootBlock = f.get(rootid);
	
	EntryPage rp = EntryPage.getEntryPageView(rootBlock);
	rp.setCount((short) 23);
	assertEquals("count", 23, rp.getCount());
	rp.setBits((byte) 3);
	assertEquals("bits", 3, rp.getBits());
	rp.setDir(3, 10);
	assertEquals("dir", 10, rp.getDir(3));
    }

    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
	junit.textui.TestRunner.run(new TestSuite(TestEntryPage.class));
    }
}
