/*
 *  $Id: LogicalRowIdManager.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
 *
 *  Logical RowID manager
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

import java.io.IOException;

/**
 *  This class manages the linked lists of logical rowid pages.
 */
final class LogicalRowIdManager {
    // our record file and associated page manager
    private RecordFile file;
    private PageManager pageman;
    private FreeLogicalRowIdPageManager freeman;

    /**
     *  Creates a log rowid manager using the indicated record file and
     *  page manager
     */
    LogicalRowIdManager(RecordFile file, PageManager pageman) 
	throws IOException {
	this.file = file;
	this.pageman = pageman;
	this.freeman = new FreeLogicalRowIdPageManager(file, pageman);
	
    }
  
    /**
     *  Creates a new logical rowid pointing to the indicated physical
     *  id
     */
    Location insert(Location loc)
    throws IOException {
	// check whether there's a free rowid to reuse
	Location retval = freeman.get();
	if (retval == null) {
	    // no. This means that we bootstrap things by allocating
	    // a new translation page and freeing all the rowids on it.
	    long firstPage = pageman.allocate(Magic.TRANSLATION_PAGE);
	    short curOffset = TranslationPage.O_TRANS;
	    for (int i = 0; i < TranslationPage.ELEMS_PER_PAGE; i++) {
		freeman.put(new Location(firstPage, curOffset));
		curOffset += PhysicalRowId.SIZE;
	    }
	    retval = freeman.get();
	    if (retval == null) {
		throw new Error("couldn't obtain free translation");
	    }
	}
	// write the translation.
	update(retval, loc);	
	return retval;
    }
    
    /**
     *  Releases the indicated logical rowid.
     */
    void delete(Location rowid)
	throws IOException {

	freeman.put(rowid);
    }

    /**
     *  Updates the mapping
     *
     *  @param rowid The logical rowid
     *  @param loc The physical rowid
     */
    void update(Location rowid, Location loc) 
	throws IOException {

	TranslationPage xlatPage = TranslationPage.getTranslationPageView(
                                      file.get(rowid.getBlock()));
	PhysicalRowId physid = xlatPage.get(rowid.getOffset());
	physid.setBlock(loc.getBlock());
	physid.setOffset(loc.getOffset());
	file.release(rowid.getBlock(), true);

    }
    
    /**
     *  Returns a mapping 
     *
     *  @param rowid The logical rowid
     *  @returns The physical rowid
     */
    Location fetch(Location rowid) 
	throws IOException {
	
	TranslationPage xlatPage = TranslationPage.getTranslationPageView(
                                      file.get(rowid.getBlock()));
	try {
	    Location retval = new Location(xlatPage.get(rowid.getOffset()));
	    return retval;
	}
	finally {
	    file.release(rowid.getBlock(), true);
	}
    }
}
