/*
 *  $Id: TestRecordFile.java,v 1.3 2001/06/02 14:32:00 boisvert Exp $
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
package jdbm.recman;

import junit.framework.*;
import java.io.File;
import java.io.IOException;

/**
 *  This class contains all Unit tests for {@link RecordFile}.
 */
public class TestRecordFile extends TestCase {

    public final static String testFileName = "test";

    public TestRecordFile(String name) {
        super(name);
    }

    public static void deleteFile(String filename) {
        System.gc();

        File f = new File(filename);

        if (f.exists()) {
            try {
                f.delete();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (f.exists()) {
                System.out.println("File still exists: " + f );
                throw new Error("Unable to delete file: "+f);
            }
        }

        /*
        int loop = 0;
        while (f.exists()) {
           loop++;
           if (loop > 5) {
               throw new Error("Unable to delete file: "+f);
           }
            try {
                f.delete();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (f.exists()) {
              System.out.println("Waiting for file "+f+" to be deleted...");
              try { Thread.currentThread().sleep(1000); } catch (Exception e) {}
              System.gc();
              if (f.exists()) {
                 new Exception("File not deleted yet: "+f).printStackTrace();
              }
            }
        }
        */
    }


    public static void deleteTestFile() {
        System.gc();

        deleteFile(testFileName);

        deleteFile(testFileName + RecordFile.extension);

        deleteFile(testFileName + TransactionManager.extension);
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
        f.close();
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

        // @alex retry to open the file
        /*
        f = new RecordFile(testFileName);
        PageManager pm = new PageManager(f);
        pm.close();
        f.close();
        */
    }


    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(new TestSuite(TestRecordFile.class));
    }
}
