/*
 *  $Id: RecordManager.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
 *
 *  Record manager
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
 *  This class manages records, which are uninterpreted blobs of data. The
 *  set of operations is simple and straightforward: you communicate with
 *  the class using long "rowids" and byte[] data blocks. Rowids are returned
 *  on inserts and you can stash them away someplace safe to be able to get
 *  back to them. Data blocks can be as long as you wish, and may have 
 *  lengths different from the original when updating.
 *  <p>
 *  Operations are synchronized, so that only one of them will happen
 *  concurrently even if you hammer away from multiple threads. Operations
 *  are made atomic by keeping a transaction log which is recovered after
 *  a crash, so the operations specified by this interface all have ACID
 *  properties. 
 *  <p>
 *  You identify a file by just the name. The package attaches <tt>.db</tt>
 *  for the database file, and <tt>.lg</tt> for the transaction log. The
 *  transaction log is synchronized regularly and then restarted, so don't
 *  worry if you see the size going up and down.
 */
public final class RecordManager {
    // our record file and assorted managers
    private RecordFile file;
    private PhysicalRowIdManager physMgr;
    private LogicalRowIdManager logMgr;
    private PageManager pageman;

    /**
     *  Creates a record manager for the indicated file
     *
     *  @throws IOException when the file cannot be opened or is not
     *          a valid file content-wise.
     */
    public RecordManager(String filename) throws IOException {
	file = new RecordFile(filename);
	pageman = new PageManager(file);
	physMgr = new PhysicalRowIdManager(file, pageman);
	logMgr = new LogicalRowIdManager(file, pageman);
    }

    /**
     *  Switches off transactioning for the record manager. This means
     *  that a) a transaction log is not kept, and b) writes aren't
     *  synch'ed after every update. This is useful when batch inserting
     *  into a new database.
     *  <p>
     *  Only call this method directly after opening the file, otherwise
     *  the results will be undefined.
     */
    public synchronized void disableTransactions() {
	file.disableTransactions();
    }
    
    
    /**
     *  Closes the record manager.
     *
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void close() throws IOException {
	pageman.close();
	file.close();
    }

    /**
     *  Inserts a new record.
     *
     *  @param data the data for the new record.
     *  @returns the rowid for the new record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized long insert(byte[] data) throws IOException {
	try {
	    Location physRowId = physMgr.insert(data);
	    return logMgr.insert(physRowId).toLong();
	} finally {
	    pageman.flush();
	}
	
    }
    
    /**
     *  Deletes a record.
     *
     *  @param rowid the rowid for the record that should be deleted.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void delete(long recid) throws IOException {
	try {
	    Location logRowId = new Location(recid);
	    Location physRowId = logMgr.fetch(logRowId);
	    physMgr.delete(physRowId);
	    logMgr.delete(logRowId);
	} finally {
	    pageman.flush();
	}
	
    }
    

    /**
     *  Updates a record.
     *
     *  @param recid the recid for the record that is to be updated.
     *  @param data the new data for the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void update(long recid, byte[] data) 
	throws IOException {

	try {
	    Location logRecid = new Location(recid);
	    Location physRecid = logMgr.fetch(logRecid);
	    Location newRecid = physMgr.update(physRecid, data);
	    if (!newRecid.equals(physRecid)) {
		logMgr.update(logRecid, newRecid);
	    }
	} finally {
	    pageman.flush();
	}
    }

    /**
     *  Fetches a record.
     *
     *  @param recid the recid for the record that must be fetched.
     *  @returns the data representing the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized byte[] fetch(long recid) throws IOException {
	return physMgr.fetch(logMgr.fetch(new Location(recid)));
    }

    /**
     *  Returns the number of slots available for "root" rowids. These slots
     *  can be used to store special rowids, like rowids that point to
     *  other rowids. Root rowids are useful for bootstrapping access to
     *  a set of data.
     */
    public int getRootCount() {
	return FileHeader.NROOTS;
    }

    /** 
     *  Returns the indicated root rowid.
     *
     *  @see getRootCount
     */
    public synchronized long getRoot(int id) throws IOException {
	return pageman.getFileHeader().getRoot(id);
    }
    
    /**
     *  Sets the indicated root rowid.
     *
     *  @see getRootCount
     */
    public synchronized void setRoot(int id, long rowid) throws IOException {
	try {
	    pageman.getFileHeader().setRoot(id, rowid);
	} finally {
	    pageman.flush();
	}
    }
}
