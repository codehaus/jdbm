/*
 *  $Id: FreePhysicalRowIdPageManager.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
 *
 *  Page manager
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
 *  This class manages free physical rowid pages and provides methods
 *  to free and allocate physical rowids on a high level.
 */
final class FreePhysicalRowIdPageManager {
    // our record file
    RecordFile file;
    // our page manager
    PageManager pageman;

    /**
     *  Creates a new instance using the indicated record file and
     *  page manager.
     */
    FreePhysicalRowIdPageManager(RecordFile file,
				 PageManager pageman) throws IOException {
	this.file = file;
	this.pageman = pageman;
    }

    /**
     *  Returns a free physical rowid of the indicated size, or
     *  null if nothing was found.
     */
    Location get(int size) throws IOException {
	
	// Loop through the free physical rowid list until we find
	// a rowid that's large enough.
	Location retval = null;
	PageCursor curs = new PageCursor(pageman, Magic.FREEPHYSIDS_PAGE);
	while (curs.next() != 0) {
	    FreePhysicalRowIdPage fp = FreePhysicalRowIdPage
		.getFreePhysicalRowIdPageView(file.get(curs.getCurrent()));
	    int slot = fp.getFirstLargerThan(size);
	    if (slot != -1) {
		// got one!
		retval =
		    new Location(fp.get(slot));
 
		int slotsize = fp.get(slot).getSize();
		fp.free(slot);
		if (fp.getCount() == 0) {
		    // page became empty - free it
		    file.release(curs.getCurrent(), false);
		    pageman.free(Magic.FREEPHYSIDS_PAGE, curs.getCurrent());
		}
		else
		    file.release(curs.getCurrent(), true);

		return retval;
	    }
	    else {
		// no luck, go to next page
		file.release(curs.getCurrent(), false);
	    }	    
	}
	return null;
    }

    /**
     *  Puts the indicated rowid on the free list
     */
    void put(Location rowid, int size)
	throws IOException {
	
	FreePhysicalRowId free = null;
	PageCursor curs = new PageCursor(pageman, Magic.FREEPHYSIDS_PAGE);
	long freePage = 0;
	while (curs.next() != 0) {
	    freePage = curs.getCurrent();
	    BlockIo curBlock = file.get(freePage);
	    FreePhysicalRowIdPage fp = FreePhysicalRowIdPage
		.getFreePhysicalRowIdPageView(curBlock);
	    int slot = fp.getFirstFree();
	    if (slot != -1) {
		free = fp.alloc(slot);
		break;
	    }

	    file.release(curBlock);
	}
	if (free == null) {
	    // No more space on the free list, add a page.
	    freePage = pageman.allocate(Magic.FREEPHYSIDS_PAGE);
	    BlockIo curBlock = file.get(freePage);
	    FreePhysicalRowIdPage fp = 
		FreePhysicalRowIdPage.getFreePhysicalRowIdPageView(curBlock);
	    free = fp.alloc(0);
	}
	
	free.setBlock(rowid.getBlock());
	free.setOffset(rowid.getOffset());
	free.setSize(size);
	file.release(freePage, true);
    }
}
