/*
 *  $Id: RecordFile.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
 *
 *  File consisting of fixed sized physical records
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
import java.util.*;

/**
 *  This class represents a random access file as a set of fixed size
 *  records. Each record has a physical record number, and records are
 *  cached in order to improve access. 
 *
 *  The set of dirty records on the in-use list constitutes a transaction. 
 *  Later on, we will send these records to some recovery thingy.
 */
final class RecordFile {
    private final TransactionManager txnMgr;

    // Todo: reorganize in hashes and fifos as necessary.
    // free -> inUse -> dirty -> inTxn -> free
    // free is a cache, thus a FIFO. The rest are hashes.
    private final LinkedList free = new LinkedList();
    private final HashMap inUse = new HashMap();
    private final HashMap dirty = new HashMap();
    private final HashMap inTxn = new HashMap();

    // transactions disabled?
    private boolean transactionsDisabled = false;

    /** The length of a single block. */
    final static int BLOCK_SIZE = 8192;//4096;

    /** The extension of a record file */
    final static String extension = ".db";

    /** A block of clean data to wipe clean pages. */
    final static byte[] cleanData = new byte[BLOCK_SIZE];

    private final RandomAccessFile file;
    private final String fileName;
    
    /**
     *  Creates a new object on the indicated filename. The file is
     *  opened in read/write mode.
     *
     *  @param fileName the name of the file to open or create, without
     *         an extension.
     *  @throws IOException whenever the creation of the underlying
     *          RandomAccessFile throws it.
     */
    RecordFile(String fileName) throws IOException {
	this.fileName = fileName;
	file = new RandomAccessFile(fileName + extension, "rw");
	txnMgr = new TransactionManager(this);
    }

    /**
     *  Returns the file name.
     */
    String getFileName() {
	return fileName;
    }

    /**
     *  Disables transactions: doesn't sync and doesn't use the
     *  transaction manager.
     */
    void disableTransactions() {
	transactionsDisabled = true;
    }
    
    /**
     *  Gets a block from the file. The returned byte array is
     *  the in-memory copy of the record, and thus can be written
     *  (and subsequently released with a dirty flag in order to
     *  write the block back).
     *
     *  @param blockid The record number to retrieve.
     */
     BlockIo get(long blockid) throws IOException {
	 Long key = new Long(blockid);

	 // try in transaction list, dirty list, free list
 
	 BlockIo node = (BlockIo) inTxn.get(key);
	 if (node != null) {
	     inTxn.remove(key);
	     inUse.put(key, node);
	     return node;
	 }
	 node = (BlockIo) dirty.get(key);
	 if (node != null) {
	     dirty.remove(key);
	     inUse.put(key, node);
	     return node;
	 }
	 for (Iterator i = free.iterator(); i.hasNext(); ) {
	     BlockIo cur = (BlockIo) i.next();
	     if (cur.getBlockId() == blockid) {
		 node = cur;
		 i.remove();
		 inUse.put(key, node);
		 return node;
	     }
	 }

	 // sanity check: can't be on in use list
	 if (inUse.get(key) != null) {
	     throw new Error("double get for block " + blockid);
	 }
	 
	 // get a new node and read it from the file
	 node = getNewNode(blockid);
	 long offset = blockid * BLOCK_SIZE;
	 if (file.length() > 0 && offset <= file.length()) {
	     file.seek(offset);
	     file.read(node.getData());
	 } else {
	     System.arraycopy(cleanData, 0, node.getData(), 0, BLOCK_SIZE);
	 }
	 inUse.put(key, node);
	 node.setClean();
	 return node;
    }

    /**
     *  Releases a block. 
     *
     *  @param blockid The record number to release.
     *  @param isDirty If true, the block was modified since the get().
     */
    void release(long blockid, boolean isDirty)
	throws IOException {
	BlockIo node = (BlockIo) inUse.get(new Long(blockid));
	if (node == null)
	    throw new IOException("bad blockid " + blockid + " on release");
	if (!node.isDirty() && isDirty)
	    node.setDirty();
	release(node);
    }

    /**
     *  Releases a block.
     *
     *  @param block The block to release.
     */
    void release(BlockIo block) {
	Long key = new Long(block.getBlockId());
	inUse.remove(key);
	if (block.isDirty())
	    dirty.put(key, block);
	else
	    if (!transactionsDisabled && block.isInTransaction())
		inTxn.put(key, block);
	    else
		free.add(block);
    }
    
    /**
     *  Commits the current transaction by flushing all dirty buffers
     *  to disk.
     */
    void commit() throws IOException {
	// debugging...
	if (!inUse.isEmpty() && inUse.size() > 1) {
	    showList(inUse.values().iterator());
	    throw new Error("in use list not empty at commit time (" 
			    + inUse.size() + ")");
	}
	//	System.out.println("committing...");
	if (!transactionsDisabled)
	    txnMgr.start();
	for (Iterator i = dirty.values().iterator(); i.hasNext(); ) {
	    BlockIo node = (BlockIo) i.next();
	    i.remove(); 
	    // System.out.println("node " + node + " map size now " + dirty.size());
	    if (transactionsDisabled) {
		long offset = node.getBlockId() * BLOCK_SIZE;
		file.seek(offset);
		file.write(node.getData());
		node.setClean();
		free.add(node);
	    }
	    else {
		txnMgr.add(node);
		inTxn.put(new Long(node.getBlockId()), node);
	    }
	}
	if (!transactionsDisabled)
	    txnMgr.commit();
    }
    
    /**
     *  Commits and closes file. 
     */
    void close() throws IOException {
	if (!dirty.isEmpty())
	    commit();
	txnMgr.shutdown();

	if (!inTxn.isEmpty()) {
	    showList(inTxn.values().iterator());
	    throw new Error("In transaction not empty");
	}
	
	// these actually ain't that bad in a production release
	if (!dirty.isEmpty()) {
	    System.out.println("ERROR: dirty blocks at close time");
	    showList(dirty.values().iterator());
	    throw new Error("Dirty blocks at close time");
	}
	if (!inUse.isEmpty()) {
	    System.out.println("ERROR: inUse blocks at close time");
	    showList(inUse.values().iterator());
	    throw new Error("inUse blocks at close time");
	}

	// debugging stuff to keep an eye on the free list
	// System.out.println("Free list size:" + free.size());
	file.close();
    }

    /**
     *  Prints contents of a list
     */
    private void showList(Iterator i) {
	int cnt = 0;
	while (i.hasNext()) {
	    System.out.println("elem " + cnt + ": " + i.next());
	    cnt++;
	}
    }
    

    /**
     *  Returns a new node. The node is retrieved (and removed)
     *  from the released list or created new.
     */
    private BlockIo getNewNode(long blockid) 
	throws IOException {

	BlockIo retval = null;
	if (!free.isEmpty()) {
	    retval = (BlockIo) free.removeFirst();
	}
	if (retval == null)
	    retval = new BlockIo(0, new byte[BLOCK_SIZE]);
	
	retval.setBlockId(blockid);
	retval.setView(null);
	return retval;
    }

    /**
     *  Synchs a node to disk. This is called by the transaction manager's
     *  synchronization code.
     */
    void synch(BlockIo node) throws IOException {
	byte[] data = node.getSnapshot();
	if (data != null) {
	    long offset = node.getBlockId() * BLOCK_SIZE;
	    file.seek(offset);
	    file.write(data);
	}
    }

    /**
     *  Releases a node from the transaction list, if it was sitting
     *  there.
     */
    void releaseFromTransaction(BlockIo node) throws IOException {
	Long key = new Long(node.getBlockId());
	if (inTxn.remove(key) != null)
	    free.add(node);
    }
    
    /**
     *  Synchronizes the file.
     */
    void sync() throws IOException {
	file.getFD().sync();
    }    
}
