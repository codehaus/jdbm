/*
 *  $Id: PhysicalRowIdManager.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
 *
 *  Physical row id manager
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
 *  This class manages physical row ids, and their data.
 */
final class PhysicalRowIdManager {

    // The file we're talking to and the associated page manager.
    private RecordFile file;
    private PageManager pageman;
    private FreePhysicalRowIdPageManager freeman;

    /**
     *  Creates a new rowid manager using the indicated record file.
     *  and page manager.
     */
    PhysicalRowIdManager(RecordFile file, PageManager pageManager) 
	throws IOException {
	this.file = file;
	this.pageman = pageManager;
	this.freeman = new FreePhysicalRowIdPageManager(file, pageman);
    }

    /**
     *  Inserts a new record. Returns the new physical rowid.
     */
    Location insert(byte[] data) throws IOException {
	Location retval = alloc(data.length);
	write(retval, data);
	return retval;
    }
    
    /**
     *  Updates an existing record. Returns the possibly changed
     *  physical rowid.
     */
    Location update(Location rowid, byte[] data)
	throws IOException {
	// fetch the record header
	BlockIo block = file.get(rowid.getBlock());
	RecordHeader head = new RecordHeader(block, rowid.getOffset());
	if (data.length > head.getAvailableSize()) {
	    // not enough space - we need to copy to a new rowid.
	    file.release(block);
	    free(rowid);
	    rowid = alloc(data.length);
	}
	else {
	    file.release(block);
	}
	
	// 'nuff space, write it in and return the rowid.
	write(rowid, data);
	return rowid;
    }
    
    /**
     *  Deletes a record.
     */
    void delete(Location rowid) throws IOException {
	free(rowid);
    }
    
    /**
     *  Retrieves a record.
     */
    byte[] fetch(Location rowid) throws IOException {
	// fetch the record header
	PageCursor curs = new PageCursor(pageman, rowid.getBlock());
	BlockIo block = file.get(curs.getCurrent());
	RecordHeader head = new RecordHeader(block, rowid.getOffset());
	
	// allocate a return buffer
	byte[] retval = new byte[head.getCurrentSize()];
	if (retval.length == 0) {
	    file.release(curs.getCurrent(), false);
	    return retval;
	}
	
	// copy bytes in
	int offsetInBuffer = 0;
	int leftToRead = retval.length;
	short dataOffset = (short) (rowid.getOffset() + RecordHeader.SIZE);
	while (leftToRead > 0) {
	    // copy current page's data to return buffer
	    int toCopy = RecordFile.BLOCK_SIZE - dataOffset;
	    if (leftToRead < toCopy)
		toCopy = leftToRead;
	    System.arraycopy(block.getData(), dataOffset,
			     retval, offsetInBuffer,
			     toCopy);
	    
	    // Go to the next block
	    leftToRead -= toCopy;
	    offsetInBuffer += toCopy;
	    
	    file.release(block);

	    if (leftToRead > 0) {
		block = file.get(curs.next());
		dataOffset = DataPage.O_DATA;
	    }
	    
	}

	return retval;
    }    

    /**
     *  Allocate a new rowid with the indicated size.
     */
    private Location alloc(int size) throws IOException {
	Location retval = freeman.get(size);
	if (retval == null)
	    retval = allocNew(size, pageman.getLast(Magic.USED_PAGE));
	return retval;
    }

    /**
     *  Allocates a new rowid. The second parameter is there to
     *  allow for a recursive call - it indicates where the search
     *  should start.
     */
    private Location allocNew(int size, long start)
	throws IOException {
	BlockIo curBlock;
	DataPage curPage;
	if (start == 0) {
	    // we need to create a new page.
	    start = pageman.allocate(Magic.USED_PAGE);
	    curBlock = file.get(start);
	    curPage = DataPage.getDataPageView(curBlock);
	    curPage.setFirst(DataPage.O_DATA);
	    RecordHeader hdr = new RecordHeader(curBlock, DataPage.O_DATA);
	    hdr.setAvailableSize(0);
	    hdr.setCurrentSize(0);
	}
	else {
	    curBlock = file.get(start);
	    curPage = DataPage.getDataPageView(curBlock);
	}

	// follow the rowids on this page to get to the last one. We don't
	// fall off, because this is the last page, remember?
	short pos = curPage.getFirst();
	if (pos == 0) {
	    // page is exactly filled by the last block of a record
	    file.release(curBlock);
	    return allocNew(size, 0);
	}
	
	RecordHeader hdr = new RecordHeader(curBlock, pos);
	while (hdr.getAvailableSize() != 0 && pos < RecordFile.BLOCK_SIZE) {
	    pos += hdr.getAvailableSize() + RecordHeader.SIZE;
	    if (pos == RecordFile.BLOCK_SIZE) {
		// Again, a filled page.
		file.release(curBlock);
		return allocNew(size, 0);
	    }
	
	    hdr = new RecordHeader(curBlock, pos);
	}

	if (pos == RecordHeader.SIZE) {
	    // I don't trust this code...
	    System.out.println("Reached?");
	    // the last record exactly filled the page. Restart forcing
	    // a new page.
	    System.out.println("rel 1");
	    file.release(curBlock);
	}
	
	// we have the position, now tack on extra pages until we've got
	// enough space.
	Location retval = new Location(start, pos);
	int freeHere = RecordFile.BLOCK_SIZE - pos - RecordHeader.SIZE;
	if (freeHere < size) {
	    // check whether the last page would have only a small bit left.
	    // if yes, increase the allocation. A small bit is a record
	    // header plus 16 bytes.
	    int lastSize = (size - freeHere) % DataPage.DATA_PER_PAGE;
	    if ((DataPage.DATA_PER_PAGE - lastSize) 
		< (RecordHeader.SIZE + 16)) {
		size += (DataPage.DATA_PER_PAGE - lastSize);
	    }

	    // write out the header now so we don't have to come back.
	    hdr.setAvailableSize(size);
	    file.release(start, true);
	    
	    int neededLeft = size - freeHere;
	    // Refactor these two blocks!
	    while (neededLeft >= DataPage.DATA_PER_PAGE) {
		start = pageman.allocate(Magic.USED_PAGE);
		curBlock = file.get(start);
		curPage = DataPage.getDataPageView(curBlock);
		curPage.setFirst((short) 0); // no rowids, just data
		file.release(start, true);
		neededLeft -= DataPage.DATA_PER_PAGE;
	    }
	    if (neededLeft > 0) {
		// done with whole chunks, allocate last fragment.
		start = pageman.allocate(Magic.USED_PAGE);
		curBlock = file.get(start);
		curPage = DataPage.getDataPageView(curBlock);
		curPage.setFirst((short) (DataPage.O_DATA + neededLeft));
		file.release(start, true);
	    }
	}
	else {
	    // just update the current page. If there's less than 16 bytes
	    // left, we increase the allocation (16 bytes is an arbitrary
	    // number).
	    if (freeHere - size <= (16 + RecordHeader.SIZE))
		size = freeHere;
	    hdr.setAvailableSize(size);
	    file.release(start, true);
	}
	return retval;	    
	
    }

    
    private void free(Location id) throws IOException {
	// get the rowid, and write a zero current size into it.
	BlockIo curBlock = file.get(id.getBlock());
	DataPage curPage = DataPage.getDataPageView(curBlock);
	RecordHeader hdr = new RecordHeader(curBlock, id.getOffset());
	hdr.setCurrentSize(0);
	file.release(id.getBlock(), true);

	// write the rowid to the free list
	freeman.put(id, hdr.getAvailableSize());
    }

    /**
     *  Writes out data to a rowid. Assumes that any resizing has been
     *  done.
     */
    private void write(Location rowid, byte[] data) 
	throws IOException {

	PageCursor curs = new PageCursor(pageman, rowid.getBlock());
	BlockIo block = file.get(curs.getCurrent());
	RecordHeader hdr = new RecordHeader(block, rowid.getOffset());
	hdr.setCurrentSize(data.length);
	if (data.length == 0) {
	    file.release(curs.getCurrent(), true);
	    return;
	}
	
	// copy bytes in
	int offsetInBuffer = 0;
	int leftToWrite = data.length;
	short dataOffset = (short) (rowid.getOffset() + RecordHeader.SIZE);
	while (leftToWrite > 0) {
	    // copy current page's data to return buffer
	    int toCopy = RecordFile.BLOCK_SIZE - dataOffset;
	    
	    if (leftToWrite < toCopy)
		toCopy = leftToWrite;
	    System.arraycopy(data, offsetInBuffer,
			     block.getData(), dataOffset,
			     toCopy);
	    
	    // Go to the next block
	    leftToWrite -= toCopy;
	    offsetInBuffer += toCopy;

	    file.release(curs.getCurrent(), true);

	    if (leftToWrite > 0) {
		block = file.get(curs.next());
		dataOffset = DataPage.O_DATA;
	    }
	}	
    }
}
    
