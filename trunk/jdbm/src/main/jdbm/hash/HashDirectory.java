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
 */

package jdbm.hash;

import jdbm.JDBMEnumeration;
import jdbm.recman.RecordManager;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Enumeration;
import java.util.Stack;
import java.util.Vector;

/**
 *  Hashtable directory page.
 *
 *  @author <a href="mailto:boisvert@exoffice.com">Alex Boisvert</a>
 *  @version $Id: HashDirectory.java,v 1.1 2000/05/06 00:00:31 boisvert Exp $
 */
public final class HashDirectory extends HashNode implements Externalizable {

    static final long serialVersionUID = 1L;

    /**
     * Maximum number of children in a directory.
     *
     * (Must be a power of 2 -- if you update this value, you must also
     *  update BIT_SIZE and MAX_DEPTH.)
     */
    public static final int MAX_CHILDREN = 256;

    /**
     * Number of significant bits per directory level.
     */
    public static final int BIT_SIZE = 8; // log2(256) = 8

    /**
     * Maximum number of levels (zero-based)
     *
     * (4 * 8 bits = 32 bits, which is the size of an "int", and as
     *  you know, hashcodes in Java are "ints")
     */
    public static final int MAX_DEPTH = 3; // 4 levels

    /** 
     * Record ids of children pages.
     */
    private long[] _children;

    /** 
     * Depth of this directory page, zero-based 
     */
    private byte _depth;

    /**
     * PageManager used to persist changes in directory and buckets
     */
    private transient RecordManager _recman;

    /**
     * This directory's record ID in the PageManager.  (transient)
     */
    private transient long _recid;


    /**
     * Public constructor used by serialization
     */
    public HashDirectory() {
        // empty
    }

    /**
     * Public constructor used by serialization.
     *
     * @arg depth Depth of this directory page.
     */
    public HashDirectory(byte depth) {
        _depth = depth;
        _children = new long[MAX_CHILDREN];
    }


    /**
     * Sets persistence context.  This method must be called before any
     * persistence-related operation.
     *
     * @arg recman RecordManager which stores this directory
     * @arg recid Record id of this directory.
     */
    void setPersistenceContext(RecordManager recman, long recid) {
        this._recman = recman;
        this._recid = recid;
    }

    /**
     * Returns whether or not this directory is empty.  A directory
     * is empty when it no longer contains buckets or sub-directories.
     */
    public boolean isEmpty() {
        for (int i=0; i<_children.length; i++) {
            if (_children[i] != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the value which is associated with the given key. Returns
     * <code>null</code> if there is not association for this key.
     *
     * @arg key key whose associated value is to be returned
     */
    Object get(Object key) 
    throws IOException {
        int hash = hashCode(key);
        long child_recid = _children[hash];
        if (child_recid == 0) {
            // not bucket/page --> not found
            return null;
        } else {
            HashNode node = null;
            try {
                node = (HashNode)_recman.fetchObject(child_recid);
                // System.out.println("HashDirectory.get() child is : "+node);
            } catch (ClassNotFoundException cnfe) {
                cnfe.printStackTrace();
                throw new Error("Unknown class for HashNode");
            }
            if (node instanceof HashDirectory) {
                // recurse into next directory level
                HashDirectory dir = (HashDirectory)node;
                dir.setPersistenceContext(_recman, child_recid);
                return dir.get(key);
            } else {
                // node is a bucket
                HashBucket bucket = (HashBucket)node;
                return bucket.getValue(key);
            }
        }
    }


    /**
     * Associates the specified value with the specified key.
     *
     * @arg key key with which the specified value is to be assocated.
     * @arg value value to be associated with the specified key.
     * @returns object which was previously associated with the given key,
     *          or <code>null</code> if no association existed.
     */
    Object put(Object key, Object value)
    throws IOException {
        if (value == null) {
            return remove(key);
        }
        int hash = hashCode(key);
        long child_recid = _children[hash];
        if (child_recid == 0) {
            // no bucket/page here yet, let's create a bucket
            HashBucket bucket = new HashBucket(_depth+1);

            // insert (key,value) pair in bucket
            Object existing = bucket.addElement(key, value);

            long b_recid = _recman.insert(bucket);
            _children[hash] = b_recid;

            _recman.update(_recid, this);

            // System.out.println("Added: "+bucket);
            return existing;
        } else {
            HashNode node;
            try {
                node = (HashNode)_recman.fetchObject(child_recid);
            } catch (ClassNotFoundException cnfe) {
                cnfe.printStackTrace();
                throw new Error("Unknown class for HashNode");
            }
            if (node instanceof HashDirectory) {
                // recursive insert in next directory level
                HashDirectory dir = (HashDirectory)node;
                dir.setPersistenceContext(_recman, child_recid);
                return dir.put(key, value);
            } else {
                // node is a bucket
                HashBucket bucket = (HashBucket)node;
                if (bucket.hasRoom()) {
                    Object existing = bucket.addElement(key, value);
                    _recman.update(child_recid, bucket);
                    // System.out.println("Added: "+bucket);
                    return existing;
                } else {
                    // overflow, so create a new directory
                    if (_depth == MAX_DEPTH) {
                        throw new Error("Cannot create deeper directory. "+
                                        "Depth="+_depth);
                    }
                    HashDirectory dir = new HashDirectory((byte)(_depth+1));
                    long dir_recid = _recman.insert(dir);
                    dir.setPersistenceContext(_recman, dir_recid);

                    _children[hash] = dir_recid;
                    _recman.update(_recid, this);

                    // discard overflown bucket
                    _recman.delete(child_recid);

                    // migrate existing bucket elements
                    Vector keys = bucket.getKeys();
                    Vector values = bucket.getValues();
                    int entries = keys.size();
                    for (int i=0; i<entries; i++) {
                        dir.put(keys.elementAt(i), values.elementAt(i));
                    }
                    
                    // (finally!) insert new element
                    return dir.put(key, value);
                }
            }
        }
    }


    /**
     * Remove the value which is associated with the given key.  If the
     * key does not exist, this method simply ignores the operation.
     *
     * @arg key key whose associated value is to be removed
     * @returns object which was associated with the given key, or
     *          <code>null</code> if no association existed with given key.
     */
    public Object remove(Object key) throws IOException {
        int hash = hashCode(key);
        long child_recid = _children[hash];
        if (child_recid == 0) {
            // not bucket/page --> not found
            return null;
        } else {
            HashNode node = null;
            try {
                node = (HashNode)_recman.fetchObject(child_recid);
                // System.out.println("HashDirectory.remove() child is : "+node);
            } catch (ClassNotFoundException cnfe) {
                cnfe.printStackTrace();
                throw new Error("Unknown class for HashNode");
            }
            if (node instanceof HashDirectory) {
                // recurse into next directory level
                HashDirectory dir = (HashDirectory)node;
                dir.setPersistenceContext(_recman, child_recid);
                Object existing = dir.remove(key);
                if (existing != null) {
                    if (dir.isEmpty()) {
                        // delete empty directory
                        _recman.delete(child_recid);
                        _children[hash] = 0;
                        _recman.update(_recid, this);
                    }
                }
                return existing;
            } else {
                // node is a bucket
                HashBucket bucket = (HashBucket)node;
                Object existing = bucket.removeElement(key);
                if (existing != null) {
                    if (bucket.getElementCount() >= 1) {
                        _recman.update(child_recid, bucket);
                    } else {
                        // delete bucket, it's empty
                        _recman.delete(child_recid);
                        _children[hash] = 0;
                        _recman.update(_recid, this);
                    }
                }
                return existing;
            }
        }
    }

    /**
     * Calculates the hashcode of a key, based on the current directory
     * depth.
     */
    private int hashCode(Object key) {
        int hashMask = hashMask();
        int hash = key.hashCode();
        hash = hash & hashMask;
        hash = hash >>> ((MAX_DEPTH - _depth) * BIT_SIZE);
        hash = hash % MAX_CHILDREN;
        /*
        System.out.println("HashDirectory.hashCode() is: 0x"
                           +Integer.toHexString(hash)
                           +" for object hashCode() 0x"
                           +Integer.toHexString(key.hashCode()));
        */
        return hash;
    }

    /**
     * Calculates the hashmask of this directory.  The hashmask is the
     * bit mask applied to a hashcode to retain only bits that are
     * relevant to this directory level.
     */
    int hashMask() {
        int bits = MAX_CHILDREN-1;
        int hashMask = bits << ((MAX_DEPTH - _depth) * BIT_SIZE);
        /*
        System.out.println("HashDirectory.hashMask() is: 0x"
                           +Integer.toHexString(hashMask));
        */
        return hashMask;
    }

    /**
     * Returns an enumeration of the keys contained in this 
     */
    public JDBMEnumeration keys()
    throws IOException {
        return new HDEnumeration(true);
    }

    /**
     * Returns an enumeration of the values contained in this 
     */
    public JDBMEnumeration values()
    throws IOException {
        return new HDEnumeration(false);
    }


    /**
     * Implement Externalizable interface
     */
    public void writeExternal(ObjectOutput out) 
    throws IOException {
        out.writeByte(_depth);
        out.writeObject(_children);
    }


    /** 
     * Implement Externalizable interface
     */
    public synchronized void readExternal(ObjectInput in) 
    throws IOException, ClassNotFoundException {
        _depth = in.readByte();
        _children = (long[])in.readObject();
    }


    ////////////////////////////////////////////////////////////////////////
    // INNER CLASS
    ////////////////////////////////////////////////////////////////////////

    /**
     * Utility class to enumerate keys/values in a HTree
     */
    public class HDEnumeration implements JDBMEnumeration {

        /**
         * True if were enumerating on keys, False if enumerating on values.
         */
        private boolean _enumerateKeys;

        /**
         * Stacks of directories & last enumerated child position
         */
        private Stack _dirStack;
        private Stack _childStack;

        /**
         * Current HashDirectory in the hierarchy
         */
        private HashDirectory _dir;

        /**
         * Current child position
         */
        private int _child;

        /**
         * Current bucket enumeration
         */
        private Enumeration enum;


        /**
         * Construct an enumeration on this directory.
         *
         * @arg enumeratedKeys True if enumeration supplies keys, False
         *                     if enumeration supplies values.
         */        
        HDEnumeration(boolean enumerateKeys) 
        throws IOException {
            _dirStack = new Stack();
            _childStack = new Stack();
            _dir = HashDirectory.this;
            _child = -1;
            _enumerateKeys = enumerateKeys;

            prepareNext();
        }

        /**
         * Returns <code>True</code> if there are more elements in this
         * enumeration.
         */
        public synchronized boolean hasMoreElements() throws IOException {
            if (enum != null) {
                return enum.hasMoreElements();
            }
            return false;
        }

        /**
         * Returns the next object in this enumeration.
         */
        public synchronized Object nextElement() throws IOException {
            if (enum != null) {
                Object next = enum.nextElement();
                if (!enum.hasMoreElements()) {
                    enum = null;
                    prepareNext();
                }
                return next;
            }
            throw new IllegalStateException("No more elements");

        }

        /**
         * Prepare internal state so we can answer <code>hasMoreElements</code>
         *
         * Actually, this code prepares an Enumeration on the next
         * Bucket to enumerate.   If no following bucket is found,
         * the next Enumeration is set to <code>null</code>.
         */
        private void prepareNext() throws IOException {
            long child_recid = 0;

            // find next bucket/directory to enumerate
            do {
                _child++;
                if (_child >= MAX_CHILDREN) {

                    if (_dirStack.isEmpty()) {
                        // no more directory in the stack, we're finished
                        return;
                    }

                    // try next page
                    _dir = (HashDirectory)_dirStack.pop();
                    _child = ((Integer)_childStack.pop()).intValue();
                }
                child_recid = _dir._children[_child];
            } while (child_recid == 0);

            if (child_recid == 0) {
                throw new Error("child_recid cannot be 0");
            }

            HashNode node = null;
            try {
                node = (HashNode)_recman.fetchObject(child_recid);
                // System.out.println("HDEnumeration.get() child is : "+node);
            } catch (ClassNotFoundException cnfe) {
                cnfe.printStackTrace();
                throw new Error("Unknown class for HashNode");
            }
            if (node instanceof HashDirectory) {
                // save current position
                _dirStack.push(_dir);
                _childStack.push(new Integer(_child));

                _dir = (HashDirectory)node;
                _child = -1;

                // recurse into 
                _dir.setPersistenceContext(_recman, child_recid);
                prepareNext();
            } else {
                // node is a bucket
                HashBucket bucket = (HashBucket)node;
                if (_enumerateKeys) {
                    enum = bucket.getKeys().elements();
                } else {
                    enum = bucket.getValues().elements();
                }
            }
        }
    }

}


