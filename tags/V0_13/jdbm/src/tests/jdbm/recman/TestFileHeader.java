/*
 *  $Id: TestFileHeader.java,v 1.1 2000/05/06 00:00:53 boisvert Exp $
 *
 *  Unit tests for FileHeader class
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
 *  This class contains all Unit tests for {@link FileHeader}.
 */
public class TestFileHeader extends TestCase {

    public TestFileHeader(String name) {
  super(name);
    }

    /**
     *  Test set, write, read
     */
    public void testSetWriteRead() throws Exception {
  BlockIo b = new BlockIo(0, new byte[1000]);
  FileHeader f = new FileHeader(b, true);
  for (int i = 0; i < Magic.NLISTS; i++) {
      f.setFirstOf(i, 100 * i);
      f.setLastOf(i, 200 * i);
  }
  
  f = new FileHeader(b, false);
  for (int i = 0; i < Magic.NLISTS; i++) {
      assertEquals("first " + i, i * 100, f.getFirstOf(i));
      assertEquals("last " + i, i * 200, f.getLastOf(i));
  }
    }

    /**
     *  Test root rowids
     */
    public void testRootRowids() throws Exception {
  BlockIo b = new BlockIo(0, new byte[RecordFile.BLOCK_SIZE]);
  FileHeader f = new FileHeader(b, true);
  for (int i = 0; i < FileHeader.NROOTS; i++) {
      f.setRoot(i, 100 * i);
  }
  
  f = new FileHeader(b, false);
  for (int i = 0; i < FileHeader.NROOTS; i++) {
      assertEquals("root " + i, i * 100, f.getRoot(i));
  }
    }

    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
  junit.textui.TestRunner.run(new TestSuite(TestFileHeader.class));
    }
}
