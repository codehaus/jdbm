/*
 *  $Id: TranslationPage.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
 *
 *  Pages containing translation from logical to physical
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
 *  Class describing a page that holds translations from physical rowids
 *  to logical rowids. In fact, the page just holds physical rowids - the
 *  page's block is the block for the logical rowid, the offset serve
 *  as offset for the rowids.
 */
final class TranslationPage extends PageHeader {
    // offsets
    static final short O_TRANS = PageHeader.SIZE; // short count
    static final short ELEMS_PER_PAGE = 
	(RecordFile.BLOCK_SIZE - O_TRANS) / PhysicalRowId.SIZE;
    
    // slots we returned.
    final PhysicalRowId[] slots = new PhysicalRowId[ELEMS_PER_PAGE];

    /**
     *  Constructs a data page view from the indicated block.
     */
    TranslationPage(BlockIo block) {
	super(block);
    }

    /**
     *  Factory method to create or return a data page for the
     *  indicated block.
     */
    static TranslationPage getTranslationPageView(BlockIo block) {
	BlockView view = block.getView();
	if (view != null && view instanceof TranslationPage)
	    return (TranslationPage) view;
	else
	    return new TranslationPage(block);
    }

    /** Returns the value of the indicated rowid on the page */
    PhysicalRowId get(short offset) {
	int slot = (offset - O_TRANS) / PhysicalRowId.SIZE;
	if (slots[slot] == null) 
	    slots[slot] = new PhysicalRowId(block, offset);
	return slots[slot];
    }
}