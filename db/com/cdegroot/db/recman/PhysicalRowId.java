/*
 *  $Id: PhysicalRowId.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
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
 *  A physical rowid is nothing else than a pointer to a physical location
 *  in a file - a (block, offset) tuple.
 *  <P>
 *  <B>Note</B>: The fact that the offset is modelled as a short limits 
 *  the block size to 32k.
 */
class PhysicalRowId {
    // offsets
    private static final short O_BLOCK = 0; // long block
    private static final short O_OFFSET = Magic.SZ_LONG; // short offset
    static final int SIZE = O_OFFSET + Magic.SZ_SHORT;
    
    // my block and the position within the block
    BlockIo block;
    short pos;

    /**
     *  Constructs a physical rowid from the indicated data starting at
     *  the indicated position.
     */
    PhysicalRowId(BlockIo block, short pos) {
	this.block = block;
	this.pos = pos;
    }

    /** Returns the block number */
    long getBlock() {
	return block.readLong(pos + O_BLOCK);
    }
    
    /** Sets the block number */
    void setBlock(long value) {
	block.writeLong(pos + O_BLOCK, value);
    }
    
    /** Returns the offset */
    short getOffset() {
	return block.readShort(pos + O_OFFSET);
    }
    
    /** Sets the offset */
    void setOffset(short value) {
	block.writeShort(pos + O_OFFSET, value);
    }
}
