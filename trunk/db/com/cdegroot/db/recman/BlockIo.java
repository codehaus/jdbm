/*
 *  $Id: BlockIo.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
 *
 *  BlockIo.java
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

/**
 *  This class wraps a page-sized byte array and provides methods
 *  to read and write data to and from it. The readers and writers
 *  are just the ones that the rest of the toolkit needs, nothing else.
 *  Values written are compatible with java.io routines. 
 *
 *  @see java.io.DataInput
 *  @see java.io.DataOutput
 */
final class BlockIo implements java.io.Serializable {
    private long blockId;
    private final byte[] snapshot; // committed snapshot.
    private boolean snapshotValid = false;
    private final transient byte[] data; // work area
    private transient BlockView view = null;
    private transient boolean dirty = false;
    private transient int transactionCount = 0;

    /**
     *  Constructs a new BlockIo instance working on the indicated
     *  buffer.
     */
    BlockIo(long blockId, byte[] data) {
	// removeme for production version
	if (blockId > 10000000000L)
	    throw new Error("bogus block id " + blockId);
	this.blockId = blockId;
	this.data = data;
	this.snapshot = new byte[data.length];
    }
    
    /**
     *  Returns the underlying array
     */
    byte[] getData() {
	return data;
    }

    /**
     *  Makes a snapshot. This sets the clean flag, because the data array
     *  is now considered clean.
     */
    void snapshot() {
	System.arraycopy(data, 0, snapshot, 0, data.length);
	snapshotValid = true;
	setClean();
    }
    
    /**
     *  Returns the snapshot. If the snapshot was already returned, returns
     *  null. By only returning a snapshot once, we make sure that the
     *  snapshot is invalidated as soon as possible, and as a side effect
     *  we'll only write a given snapshot once.
     */
    synchronized byte[] getSnapshot() {
	if (snapshotValid) {
	    snapshotValid = false;
	    return snapshot;
	}
	else {
	    return null;
	}
    }

    /**
     *  Sets the block number. Should only be called by RecordFile.
     */
    void setBlockId(long id) {
	if (isInTransaction())
	    throw new Error("BlockId assigned for transaction block");
	// removeme for production version
	if (id > 10000000000L)
	    throw new Error("bogus block id " + id);
	blockId = id;
    }

    /**
     *  Returns the block number.
     */
    long getBlockId() {
	return blockId;
    }
    
    /**
     *  Returns the current view of the block.
     */
    BlockView getView() {
	return view;
    }

    /**
     *  Sets the current view of the block.
     */
    void setView(BlockView view) {
	this.view = view;
    }

    /**
     *  Sets the dirty flag
     */
    void setDirty() {
	dirty = true;
    }
    
    /**
     *  Clears the dirty flag
     */
    void setClean() {
	dirty = false;
    }
    
    /**
     *  Returns true if the dirty flag is set.
     */
    boolean isDirty() {
	return dirty;
    }

    /**
     *  Returns true if the block is still dirty with respect to the
     *  transaction log. 
     */
    boolean isInTransaction() {
	return transactionCount != 0;
    }
    
    /**
     *  Increments transaction count for this block, to signal that this
     *  block is in the log but not yet in the data file. The method also
     *  takes a snapshot so that the data may be modified in new transactions.
     */
    synchronized void incrementTransactionCount() {
	snapshot();
	transactionCount++;
    }
    
    /**
     *  Decrements transaction count for this block, to signal that this
     *  block has been written from the log to the data file.
     */
    synchronized void decrementTransactionCount() {
	transactionCount--;
	if (transactionCount < 0) 
	    throw new Error("transaction count on block " 
			    + getBlockId() + " below zero!");
    }

    /**
     *  Reads a short from the indicated position
     */
    short readShort(int pos) {
	return (short)
	    (((short) (data[pos+0] & 0xff) << 8) | 
	     ((short) (data[pos+1] & 0xff) << 0));
	
    }
    
    /**
     *  Writes a short to the indicated position
     */
    void writeShort(int pos, short value) {
	data[pos+0] = (byte)(0xff & (value >> 8));
	data[pos+1] = (byte)(0xff & (value >> 0));
	setDirty();
    }
    
    /**
     *  Reads an int from the indicated position
     */
    int readInt(int pos) {
	return
	    (((int)(data[pos+0] & 0xff) << 24) |
	     ((int)(data[pos+1] & 0xff) << 16) |
	     ((int)(data[pos+2] & 0xff) <<  8) |
	     ((int)(data[pos+3] & 0xff) <<  0));
    }

    /**
     *  Writes an int to the indicated position
     */
    void writeInt(int pos, int value) {
	data[pos+0] = (byte)(0xff & (value >> 24));
	data[pos+1] = (byte)(0xff & (value >> 16));
	data[pos+2] = (byte)(0xff & (value >>  8));
	data[pos+3] = (byte)(0xff & (value >>  0));
	setDirty();
    }

    /**
     *  Reads a long from the indicated position
     */
    long readLong(int pos) {
	return
	    (((long)(data[pos+0] & 0xff) << 56) |
	     ((long)(data[pos+1] & 0xff) << 48) |
	     ((long)(data[pos+2] & 0xff) << 40) |
	     ((long)(data[pos+3] & 0xff) << 32) |
	     ((long)(data[pos+4] & 0xff) << 24) |
	     ((long)(data[pos+5] & 0xff) << 16) |
	     ((long)(data[pos+6] & 0xff) <<  8) |
	     ((long)(data[pos+7] & 0xff) <<  0));
    }

    /**
     *  Writes a long to the indicated position
     */
    void writeLong(int pos, long value) {
	data[pos+0] = (byte)(0xff & (value >> 56));
	data[pos+1] = (byte)(0xff & (value >> 48));
	data[pos+2] = (byte)(0xff & (value >> 40));
	data[pos+3] = (byte)(0xff & (value >> 32));
	data[pos+4] = (byte)(0xff & (value >> 24));
	data[pos+5] = (byte)(0xff & (value >> 16));
	data[pos+6] = (byte)(0xff & (value >>  8));
	data[pos+7] = (byte)(0xff & (value >>  0));
	setDirty();
    }
    
    // overrides java.lang.Object

    public String toString() {
	return "BlockIO(" 
	    + blockId + ","
	    + dirty + ","
	    + view + ")";
    }
}
