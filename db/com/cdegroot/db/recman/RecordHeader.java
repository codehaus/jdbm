/*
 *  $Id: RecordHeader.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
 *
 *  Physical row id's
 *
 *  Simple db toolkit.
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

/**
 *  The data that comes at the start of a record of data. It stores 
 *  both the current size and the avaliable size for the record - the latter
 *  can be bigger than the former, which allows the record to grow without
 *  needing to be moved and which allows the system to put small records
 *  in larger free spots.
 */
class RecordHeader {
    // offsets
    private static final short O_CURRENTSIZE = 0; // int currentSize
    private static final short O_AVAILABLESIZE = Magic.SZ_INT; // int availableSize
    static final int SIZE = O_AVAILABLESIZE + Magic.SZ_INT;
    
    // my block and the position within the block
    private BlockIo block;
    private short pos;

    /**
     *  Constructs a record header from the indicated data starting at
     *  the indicated position.
     */
    RecordHeader(BlockIo block, short pos) {
	this.block = block;
	this.pos = pos;
	if (pos > (RecordFile.BLOCK_SIZE - SIZE))
	    throw new Error("Offset too large for record header (" 
			    + block.getBlockId() + ":" 
			    + pos + ")");
    }

    /** Returns the current size */
    int getCurrentSize() {
	return block.readInt(pos + O_CURRENTSIZE);
    }
    
    /** Sets the current size */
    void setCurrentSize(int value) {
	block.writeInt(pos + O_CURRENTSIZE, value);
    }
    
    /** Returns the available size */
    int getAvailableSize() {
	return block.readInt(pos + O_AVAILABLESIZE);
    }
    
    /** Sets the available size */
    void setAvailableSize(int value) {
	block.writeInt(pos + O_AVAILABLESIZE, value);
    }

    // overrides java.lang.Object
    public String toString() {
	return "RH(" + block.getBlockId() + ":" + pos 
	    + ", avl=" + getAvailableSize()
	    + ", cur=" + getCurrentSize() 
	    + ")";
    }
}
