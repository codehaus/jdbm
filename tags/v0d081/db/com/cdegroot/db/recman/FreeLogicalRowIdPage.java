/*
 *  $Id: FreeLogicalRowIdPage.java,v 1.2 2000/04/11 06:07:42 boisvert Exp $
 *
 *  Logical row id pages
 *
 *  Simple db toolkit.
 *  Copyright (C) 1999,2000 Cees de Groot <cg@cdegroot.com>
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
 *  Class describing a page that holds logical rowids that were freed. Note
 *  that the methods have *physical* rowids in their signatures - this is
 *  because logical and physical rowids are internally the same, only their
 *  external representation (i.e. in the client API) differs.
 */
class FreeLogicalRowIdPage extends PageHeader {
    // offsets
    private static final short O_COUNT = PageHeader.SIZE; // short count
    static final short O_FREE = (short)(O_COUNT + Magic.SZ_SHORT);
    static final short ELEMS_PER_PAGE = (short)
	((RecordFile.BLOCK_SIZE - O_FREE) / PhysicalRowId.SIZE);

    // slots we returned.
    final PhysicalRowId[] slots = new PhysicalRowId[ELEMS_PER_PAGE];

    /**
     *  Constructs a data page view from the indicated block.
     */
    FreeLogicalRowIdPage(BlockIo block) {
	super(block);
    }

    /**
     *  Factory method to create or return a data page for the
     *  indicated block.
     */
    static FreeLogicalRowIdPage getFreeLogicalRowIdPageView(BlockIo block) {

	BlockView view = block.getView();
	if (view != null && view instanceof FreeLogicalRowIdPage)
	    return (FreeLogicalRowIdPage) view;
	else
	    return new FreeLogicalRowIdPage(block);
    }

    /** Returns the number of free rowids */
    short getCount() {
	return block.readShort(O_COUNT);
    }

    /** Sets the number of free rowids */
    private void setCount(short i) {
	block.writeShort(O_COUNT, i);
    }

    /** Frees a slot */
    void free(int slot) {
	get(slot).setBlock(0);
	setCount((short) (getCount() - 1));
    }

    /** Allocates a slot */
    PhysicalRowId alloc(int slot) {
	setCount((short) (getCount() + 1));
	get(slot).setBlock(-1);
	return get(slot);
    }

    /** Returns true if a slot is allocated */
    boolean isAllocated(int slot) {
	return get(slot).getBlock() > 0;
    }

    /** Returns true if a slot is free */
    boolean isFree(int slot) {
	return !isAllocated(slot);
    }


    /** Returns the value of the indicated slot */
    PhysicalRowId get(int slot) {
	if (slots[slot] == null)
	    slots[slot] = new PhysicalRowId(block, slotToOffset(slot));;
	return slots[slot];
    }

    /** Converts slot to offset */
    private short slotToOffset(int slot) {
	return (short) (O_FREE +
	    (slot * PhysicalRowId.SIZE));
    }

    /**
     *  Returns first free slot, -1 if no slots are available
     */
    int getFirstFree() {
	for (int i = 0; i < ELEMS_PER_PAGE; i++) {
	    if (isFree(i))
		return i;
	}
	return -1;
    }

    /**
     *  Returns first allocated slot, -1 if no slots are available.
     */
    int getFirstAllocated() {
	for (int i = 0; i < ELEMS_PER_PAGE; i++) {
	    if (isAllocated(i))
		return i;
	}
	return -1;
    }
}
