/*
 *  $Id: TestExtendibleHashSet.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
 *
 *  Unit tests for ExtendibleHashSet class
 *
 *  Simple db toolkit
 *  Copyright (C) 2000 Cees de Groot <cg@cdegroot.com>
 */
package com.cdegroot.db.hash;

import junit.framework.*;

/**
 *  This class contains all Unit tests for {@link ExtendibleHashSet}.
 */
public class TestExtendibleHashSet extends TestCase {

    final static String testFileName = "test";

    public TestExtendibleHashSet(String name) {
	super(name);
    }
    
    /**
     *  Test ctor
     */
    public void testCtor() {
	new ExtendibleHashSet();
    }
    

    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
	junit.textui.TestRunner.run(new TestSuite(TestExtendibleHashSet.class));
    }
}
