/*
 *  $Id: EntryPage.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
 *
 *  Root page for a hash
 *
 *  Simple db toolkit.
 *  Copyright (C) 1999, 2000 Cees de Groot <cg@cdegroot.com>
 */
package com.cdegroot.db.hash;

import com.cdegroot.db.recman.*;

/**
 *  The three kinds of pages in the extendible hash - root, directory,
 *  and entry - are all represented by this class.
 */
final class EntryPage extends PageHeader {
    // offsets
    private static final short O_COUNT = PageHeader.SIZE; // int count
    private static final short O_BITS = O_COUNT + Magic.SZ_INT; // byte bits
    private static final short O_DIR = O_BITS + Magic.SZ_BYTE;
    private static final short ELEMS_PER_PAGE = 
	(RecordFile.BLOCK_SIZE - O_DIR) / Magic.SZ_LONG;
    
    /**
     *  Constructs a page view from the indicated block.
     */
    EntryPage(BlockIo block) {
	super(block);
    }

    /**
     *  Factory method to create or return a data page for the
     *  indicated block.
     */
    static EntryPage getEntryPageView(BlockIo block) {
	BlockView view = block.getView();
	if (view != null && view instanceof EntryPage)
	    return (EntryPage) view;
	else
	    return new EntryPage(block);
    }

    /** Returns the total number of entries "under" the page */
    short getCount() {
	return block.readShort(O_COUNT);
    }

    /** Sets the number of entries */
    void setCount(short i) {
	block.writeShort(O_COUNT, i);
    }

    /** Returns the bit depth for the directory */
    byte getBits() {
	return block.readByte(O_BITS);
    }
    
    /** Sets the bit depth for the directory */
    void setBits(byte b) {
	block.writeByte(O_BITS, b);
    }
    
    /** Returns the indicated entry */
    long getDir(int i) {
	return block.readLong(entryToOffset(i));
    }

    /** Sets the indicated entry */
    void setDir(int i, long recid) {
	block.writeLong(entryToOffset(i), recid);
    }

    /** Converts number to offset */
    private short entryToOffset(int i) {
	return (short) (O_DIR + (i * Magic.SZ_LONG));
    }
}
