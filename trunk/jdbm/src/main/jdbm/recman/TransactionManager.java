/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot.
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2000 (C) Cees de Groot. All Rights Reserved.
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 * $Id: TransactionManager.java,v 1.4 2001/04/05 07:04:29 boisvert Exp $
 */

package jdbm.recman;

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

    // We keep 10 transactions in the log file before closing it.
    static final int TXNS_IN_LOG = 10;

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
    private void synchronizeLogFromMemory() throws IOException {
        close();
        for (int i = 0; i < TXNS_IN_LOG; i++) {
            if (txns[i] == null)
                continue;
            synchronizeBlocks(txns[i], true);
            txns[i] = null;
        }
        owner.sync();
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
        } catch (IOException e) {
            // corrupted/empty logfile
            logFile.delete();
            return;
        }

        while (true) {
            ArrayList blocks = null;
            try {
                blocks = (ArrayList) ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new Error("Unexcepted exception: " + e);
            } catch (IOException e) {
                // corrupted logfile, ignore rest of transactions
                break;
            }
            synchronizeBlocks(blocks, false);

            // ObjectInputStream must match exactly each
            // ObjectOutputStream created during writes
            try {
                ois = new ObjectInputStream(fis);
            } catch (IOException e) {
                // corrupted logfile, ignore rest of transactions
                break;
            }
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
                if (!cur.isInTransaction()) {
                    owner.releaseFromTransaction(cur, true);
                }
            }
        }
    }

    /** Set clean flag on the blocks. */
    private void setClean(ArrayList blocks)
    throws IOException {
        for (Iterator k = blocks.iterator(); k.hasNext(); ) {
            BlockIo cur = (BlockIo) k.next();
            cur.setClean();
        }
    }

    /** Discards the indicated blocks and notify the owner. */
    private void discardBlocks(ArrayList blocks)
    throws IOException {
        for (Iterator k = blocks.iterator(); k.hasNext(); ) {
            BlockIo cur = (BlockIo) k.next();
            cur.decrementTransactionCount();
            if (!cur.isInTransaction()) {
                owner.releaseFromTransaction(cur, false);
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
            synchronizeLogFromMemory();
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

        // set clean flag to indicate blocks have been written to log
        setClean(txns[curTxn]);

        // open a new ObjectOutputStream in order to store
        // newer states of BlockIo
        oos = new ObjectOutputStream(fos);
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
        synchronizeLogFromMemory();
        close();
    }

    /**
     *  Closes open files.
     */
    private void close() throws IOException {
        sync();
        oos.close();
        fos.close();
        oos = null;
        fos = null;
    }

    /**
     * Force closing the file without synchronizing pending transaction data.
     * Used for testing purposes only.
     */
    void forceClose() throws IOException {
        oos.close();
        fos.close();
        oos = null;
        fos = null;
    }

    /**
     * Use the disk-based transaction log to synchronize the data file.
     * Outstanding memory logs are discarded because they are believed
     * to be inconsistent.
     */
    void synchronizeLogFromDisk() throws IOException {
        close();

        for (int i = 0; i < TXNS_IN_LOG; i++) {
            if (txns[i] == null)
                continue;
            discardBlocks(txns[i]);
            txns[i] = null;
        }

        recover();
        open();
    }

}
