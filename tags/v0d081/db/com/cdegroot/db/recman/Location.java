/*
 *  $Id: Location.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
 *
 *  Location within file
 *
 *  Simple db kit.
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
 *  This class represents a location within a file. Both physical and
 *  logical rowids are based on locations internally - this version is
 *  used when there is no file block to back the location's data.
 */
final class Location {
    private long block;
    private short offset;

    /**
     *  Creates a location from a (block, offset) tuple.
     */
    Location(long block, short offset) {
	this.block = block;
	this.offset = offset;
    }

    /**
     *  Creates a location from a combined block/offset long, as 
     *  used in the external representation of logical rowids.
     *
     *  @see toLong()
     */
    Location(long blockOffset) {
	this.offset = (short) (blockOffset & 0xffff);
	this.block = blockOffset >> 16;
    }

    /**
     *  Creates a location based on the data of the physical rowid.
     */
    Location(PhysicalRowId src) {
	block = src.getBlock();
	offset = src.getOffset();
    }

    /** Returns the file block of the location */
    long getBlock() {
	return block;
    }
    /** Returns the offset within the block of the location */
    short getOffset() {
	return offset;
    }

    /**
     *  Returns the external representation of a location when used
     *  as a logical rowid, which combines the block and the offset
     *  in a single long.
     */
    long toLong() {
	return (block << 16) + (long) offset;
    }

    // overrides of java.lang.Object

    public boolean equals(Object o) {
	if (o == null || !(o instanceof Location))
	    return false;
	Location ol = (Location) o;
	return ol.block == block && ol.offset == offset;
    }
    public String toString() {
	return "PL(" + block + ":" + offset + ")";
    }
}
