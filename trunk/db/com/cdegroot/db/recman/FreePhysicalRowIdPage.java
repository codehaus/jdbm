/*
 *  $Id: FreePhysicalRowIdPage.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
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
 *  Class describing a page that holds physical rowids that were freed.
 */
final class FreePhysicalRowIdPage extends PageHeader {
    // offsets
    private static final short O_COUNT = PageHeader.SIZE; // short count
    static final short O_FREE = O_COUNT + Magic.SZ_SHORT;
    static final short ELEMS_PER_PAGE = 
	(RecordFile.BLOCK_SIZE - O_FREE) / FreePhysicalRowId.SIZE;
    
    // slots we returned.
    FreePhysicalRowId[] slots = new FreePhysicalRowId[ELEMS_PER_PAGE];

    /**
     *  Constructs a data page view from the indicated block.
     */
    FreePhysicalRowIdPage(BlockIo block) {
	super(block);
    }

    /**
     *  Factory method to create or return a data page for the
     *  indicated block.
     */
    static FreePhysicalRowIdPage getFreePhysicalRowIdPageView(BlockIo block) {
	BlockView view = block.getView();
	if (view != null && view instanceof FreePhysicalRowIdPage)
	    return (FreePhysicalRowIdPage) view;
	else
	    return new FreePhysicalRowIdPage(block);
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
	get(slot).setSize(0);
	setCount((short) (getCount() - 1));
    }

    /** Allocates a slot */
    FreePhysicalRowId alloc(int slot) {
	setCount((short) (getCount() + 1));
	return get(slot);
    }

    /** Returns true if a slot is allocated */
    boolean isAllocated(int slot) {
	return get(slot).getSize() != 0;
    }

    /** Returns true if a slot is free */
    boolean isFree(int slot) {
	return !isAllocated(slot);
    }
    
    
    /** Returns the value of the indicated slot */
    FreePhysicalRowId get(int slot) {
	if (slots[slot] == null) 
	    slots[slot] = new FreePhysicalRowId(block, slotToOffset(slot));;
	return slots[slot];
    }

    /** Converts slot to offset */
    short slotToOffset(int slot) {
	return (short) (O_FREE +
	    (slot * FreePhysicalRowId.SIZE));
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
     *  Returns first slot with available size >= indicated size,  
     *  or -1 if no slots are available.
     **/
    int getFirstLargerThan(int size) {
	for (int i = 0; i < ELEMS_PER_PAGE; i++) {
	    if (isAllocated(i) && get(i).getSize() >= size)
		return i;
	}
	return -1;
    }
}
