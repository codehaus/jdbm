/*
 *  $Id: TransactionManager.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
 *
 *  Transaction manager
 *
 *  Simple db toolkit.
 *  Copyright (C) 2000 Cees de Groot <cg@cdegroot.com>
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
import java.util.*;

/**
 *  This class manages the transaction log that belongs to every 
 *  {@link RecordFile}. The transaction log is either clean, or
 *  in progress. In the latter case, the transaction manager
 *  takes care of a roll forward. 
 *
 *  Implementation note: this is a proof-of-concept implementation
 *  which hasn't been optimized for speed. For instance, all sorts
 *  of streams are created for every transaction.
 */
// TODO: Handle the case where we are recovering lg9 and lg0, were we
// should start with lg9 instead of lg0!

final class TransactionManager {
    private RecordFile owner;
    
    // streams for transaction log.
    private FileOutputStream fos;
    private ObjectOutputStream oos;

    // We keep 100 transactions in the log file before closing it.
    static final int TXNS_IN_LOG = 100;

    // In-core copy of transactions. We could read everything back from
    // the log file, but the RecordFile needs to keep the dirty blocks in
    // core anyway, so we might as well point to them and spare us a lot
    // of hassle.
    private ArrayList[] txns = new ArrayList[TXNS_IN_LOG];
    private int curTxn = -1;

    /** Extension of a log file. */
    static final String extension = ".lg";

    /**
     *  Instantiates a transaction manager instance. If recovery
     *  needs to be performed, it is done.
     *
     *  @param owner the RecordFile instance that owns this transaction mgr.
     *  @param fileName the name of the file, without extension.
     */
    TransactionManager(RecordFile owner) throws IOException {
	this.owner = owner;
	recover();
	open();
    }

    /** Builds logfile name  */
    private String makeLogName() {
	return owner.getFileName() + extension;
    }

    /** Synchs in-core transactions to data file and opens a fresh log */
    private void synchronizeLog() throws IOException {
	close();	
	for (int i = 0; i < TXNS_IN_LOG; i++) {
	    if (txns[i] == null)
		continue;
	    synchronizeBlocks(txns[i], true);
	    txns[i] = null;
	}
	open();
    }
    
    /** Opens the log file */
    private void open() throws IOException {
	fos = new FileOutputStream(makeLogName());
	oos = new ObjectOutputStream(fos);
	oos.writeShort(Magic.LOGFILE_HEADER);
	oos.flush();
	curTxn = -1;
    }

    /** Startup recovery on all files */
    private void recover() throws IOException {
	
	String logName = makeLogName();
	File logFile = new File(logName);
	if (!logFile.exists())
	    return;
	if (logFile.length() == 0) {
	    logFile.delete();
	    return;
	}

	FileInputStream fis = new FileInputStream(logFile);
	ObjectInputStream ois = new ObjectInputStream(fis);

	try {
	    if (ois.readShort() != Magic.LOGFILE_HEADER)
		throw new Error("Bad magic on log file");
	} catch (EOFException e) {
	    // Apparently a very empty logfile. Can happen...
	    logFile.delete();
	    return;
	}

	while (true) {
	    ArrayList blocks = null;
	    try {
		blocks = (ArrayList) ois.readObject();
	    } catch (ClassNotFoundException e) {
		throw new Error("Unexcepted exception: " + e);
	    } catch (EOFException e) {
		break;
	    }
	    synchronizeBlocks(blocks, false);
	}
	owner.sync();
	logFile.delete();
    }

    /** Synchronizes the indicated blocks with the owner. */
    private void synchronizeBlocks(ArrayList blocks, boolean fromCore) 
	throws IOException {
	// write block vector elements to the data file.
	for (Iterator k = blocks.iterator(); k.hasNext(); ) {
	    BlockIo cur = (BlockIo) k.next();
	    owner.synch(cur);
	    if (fromCore) {
		cur.decrementTransactionCount();
		if (!cur.isInTransaction())
		    owner.releaseFromTransaction(cur);
	    }
	}
    }

    /**
     *  Starts a transaction. This can block if all slots have been filled
     *  with full transactions, waiting for the synchronization thread to
     *  clean out slots.
     */
    void start() throws IOException {
	curTxn++;
	if (curTxn == TXNS_IN_LOG) {
	    synchronizeLog();
	    curTxn = 0;
	}
	txns[curTxn] = new ArrayList();
    }
    
    /**
     *  Indicates the block is part of the transaction.
     */
    void add(BlockIo block) throws IOException {
	block.incrementTransactionCount();
	txns[curTxn].add(block);
    }
    
    /**
     *  Commits the transaction to the log file.
     */
    void commit() throws IOException {
	oos.writeObject(txns[curTxn]);
	sync();
    }
    
    /** Flushes and syncs */
    private void sync() throws IOException {
	oos.flush();
	fos.flush();
	fos.getFD().sync();
    }

    /**
     *  Shutdowns the transaction manager. Resynchronizes outstanding
     *  logs.
     */
    void shutdown() throws IOException {
	synchronizeLog();
	close();
    }
    
    /**
     *  Closes open files.
     */
    private void close() throws IOException {
	sync();
	oos.close();
	fos.close();
    }
}
