/*
 *  $Id: DataPage.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
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
 *  Class describing a page that holds data.
 */
final class DataPage extends PageHeader {
    // offsets
    private static final short O_FIRST = PageHeader.SIZE; // short firstrowid
    static final short O_DATA = O_FIRST + Magic.SZ_SHORT;
    static final short DATA_PER_PAGE = RecordFile.BLOCK_SIZE - O_DATA;

    /**
     *  Constructs a data page view from the indicated block.
     */
    DataPage(BlockIo block) {
	super(block);
    }

    /**
     *  Factory method to create or return a data page for the
     *  indicated block.
     */
    static DataPage getDataPageView(BlockIo block) {
	BlockView view = block.getView();
	if (view != null && view instanceof DataPage)
	    return (DataPage) view;
	else
	    return new DataPage(block);
    }

    /** Returns the first rowid's offset */
    short getFirst() {
	return block.readShort(O_FIRST);
    }
    
    /** Sets the first rowid's offset */
    void setFirst(short value) {
	paranoiaMagicOk();
	if (value > 0 && value < O_DATA)
	    throw new Error("DataPage.setFirst: offset " + value 
			    + " too small");
	block.writeShort(O_FIRST, value);
    }
}
