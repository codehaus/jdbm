/*
 *  $Id: FreePhysicalRowId.java,v 1.2 2000/04/11 06:07:07 boisvert Exp $
 *
 *  Physical row id's on free list
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
 *  This class extends the physical rowid with a size value to indicated
 *  the size of a free rowid on the free rowid list.
 */
final class FreePhysicalRowId extends PhysicalRowId {
    // offsets
    private static final short O_SIZE = PhysicalRowId.SIZE; // int size
    static final short SIZE = O_SIZE + Magic.SZ_INT;

    /**
     *  Constructs a physical rowid from the indicated data starting at
     *  the indicated position.
     */
    FreePhysicalRowId(BlockIo block, short pos) {
	super(block, pos);
    }

    /** Returns the size */
    int getSize() {
	return block.readInt(pos + O_SIZE);
    }

    /** Sets the size */
    void setSize(int value) {
	block.writeInt(pos + O_SIZE, value);
    }

}
