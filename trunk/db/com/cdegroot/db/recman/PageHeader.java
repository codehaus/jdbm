/*
 *  $Id: PageHeader.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
 *
 *  PageHeader.java
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
package com.cdegroot.db.recman;

import java.io.*;

/**
 *  This class represents a page header. It is the common superclass for
 *  all different page views.
 */
class PageHeader implements BlockView {
    // offsets
    private static final short O_MAGIC = 0; // short magic
    private static final short O_NEXT = Magic.SZ_SHORT;  // long next
    private static final short O_PREV = O_NEXT + Magic.SZ_LONG; // long prev
    static final int SIZE = O_PREV + Magic.SZ_LONG;
    
    // my block
    BlockIo block;
    
    /**
     *  Constructs a PageHeader object from a block
     *
     *  @param block The block that contains the file header
     *  @throws IOException if the block is too short to keep the file
     *          header.
     */
    PageHeader(BlockIo block) {
	initialize(block);
	if (!magicOk()) 
	    throw new Error("CRITICAL: page header magic for block " 
			    + block.getBlockId() + " not OK " 
			    + getMagic());
    }
    
    /**
     *  Constructs a new PageHeader of the indicated type. Used for newly
     *  created pages.
     */
    PageHeader(BlockIo block, short type) {
	initialize(block);
	setType(type);
    }

    /**
     *  Factory method to create or return a page header for the
     *  indicated block.
     */
    static PageHeader getView(BlockIo block) {
	BlockView view = block.getView();
	if (view != null && view instanceof PageHeader)
	    return (PageHeader) view;
	else
	    return new PageHeader(block);
    }

    private void initialize(BlockIo block) {
	this.block = block;
	block.setView(this);
    }

    /**
     *  Returns true if the magic corresponds with the fileHeader magic.
     */
    private boolean magicOk() {
	int magic = getMagic();
	return magic >= Magic.BLOCK 
	    && magic <= (Magic.BLOCK + Magic.FREEPHYSIDS_PAGE);
    }

    /**
     *  For paranoia mode 
     */
    protected void paranoiaMagicOk() {
	if (!magicOk())
	    throw new Error("CRITICAL: page header magic not OK " 
			    + getMagic());
    }
    
    /** Returns the magic code */
    short getMagic() {
	return block.readShort(O_MAGIC);
    }
    
    /** Returns the next block. */
    long getNext() {
	paranoiaMagicOk();
	return block.readLong(O_NEXT);
    }
    
    /** Sets the next block. */
    void setNext(long next) {
	paranoiaMagicOk();
	block.writeLong(O_NEXT, next);
    }

    /** Returns the previous block. */
    long getPrev() {
	paranoiaMagicOk();
	return block.readLong(O_PREV);
    }
    
    /** Sets the previous block. */
    void setPrev(long prev) {
	paranoiaMagicOk();
	block.writeLong(O_PREV, prev);
    }

    /** Sets the type of the page header */
    void setType(short type) {
	block.writeShort(O_MAGIC, (short) (Magic.BLOCK + type));
    }
}
