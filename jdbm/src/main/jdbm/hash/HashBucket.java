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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Vector;

/**
 * A bucket is a placeholder for multiple (key, value) pairs.  Buckets
 * are used to store collisions (same hash value) at all levels of an
 * H*tree.
 *
 * There are two types of buckets: leaf and non-leaf.
 *
 * Non-leaf buckets are buckets which hold collisions which happen
 * when the H*tree is not fully expanded.   Keys in a non-leaf buckets
 * can have different hash codes.  Non-leaf buckets are limited to an
 * arbitrary size.  When this limit is reached, the H*tree should create
 * a new Directory page and distribute keys of the non-leaf buckets into
 * the newly created Directory.
 *
 * A leaf bucket is a bucket which contains keys which all have
 * the same <code>hashCode()</code>.  Leaf buckets stand at the
 * bottom of an H*tree because the hashing algorithm cannot further
 * discriminate between different keys based on their hash code.
 *
 */
final class HashBucket extends HashNode implements Externalizable {
    final static long serialVersionUID = 1L;

    /**
     * The maximum number of elements (key, value) a non-leaf bucket 
     * can contain.
     */
    public static final int OVERFLOW_SIZE = 8;

    /**
     * Depth of this bucket.
     */
    private int _depth;

    /**
     * Keys in this bucket.  Keys are ordered to match their respective
     * value in <code>_values</code>.
     */
    private Vector _keys;

    /**
     * Values in this bucket.  Values are ordered to match their respective
     * key in <code>_keys</code>.
     */
    private Vector _values;


    /** 
     * Public constructor for serialization.
     */
    public HashBucket() {
        // empty
    }

    /** 
     * Construct a bucket with a given depth level.  Depth level is the
     * number of <code>HashDirectory</code> above this bucket.
     */
    public HashBucket(int level) {
        if (level > HashDirectory.MAX_DEPTH+1) {
            throw new Error("Cannot create bucket with depth > MAX_DEPTH+1. "
                            +"Depth="+level);
        }
        this._depth = level;
        this._keys = new Vector(OVERFLOW_SIZE);
        this._values = new Vector(OVERFLOW_SIZE);
    }

    /**
     * Returns the number of elements contained in this bucket.
     */
    public int getElementCount() {
        return _keys.size();
    }

    /**
     * Returns whether or not this bucket is a "leaf bucket".
     */
    public boolean isLeaf() {
        return (_depth > HashDirectory.MAX_DEPTH);
    }

    /**
     * Returns true if bucket can accept at least one more element.
     */
    public boolean hasRoom() {
        if (isLeaf()) {
            return true;  // leaf buckets are never full
        } else {
            // non-leaf bucket
            return (_keys.size() < OVERFLOW_SIZE);
        }
    }


    /**
     * Add an element (key, value) to this bucket.  If an existing element 
     * has the same key, it is replaced silently.
     *
     * @returns Object which was previously associated with the given key
     *          or <code>null</code> if no association existed.
     */
    public Object addElement(Object key, Object value) {
        int existing = _keys.indexOf(key);
        if (existing != -1) {
            // replace existing element
            Object before = _values.elementAt(existing);
            _values.setElementAt(value, existing);
            return before;
        } else {
            // add new (key, value) pair
            _keys.addElement(key);
            _values.addElement(value);
            return null;
        }
    }

    /**
     * Remove an element, given a specific key.
     *
     * @returns Removed element value, or <code>null</code> if not found
     */
    public Object removeElement(Object key) {
        int existing = _keys.indexOf(key);
        if (existing != -1) {
            Object obj = _values.elementAt(existing);
            _keys.removeElementAt(existing);
            _values.removeElementAt(existing);
            return obj;
        } else {
            // not found
            return null;
        }
    }

    /**
     * Returns the value associated with a given key.  If the given key
     * is not found in this bucket, returns <code>null</code>.
     */
    public Object getValue(Object key) {
        int existing = _keys.indexOf(key);
        if (existing != -1) {
            return _values.elementAt(existing);
        } else {
            // key not found
            return null;
        }
    }


    /**
     * Obtain keys contained in this buckets.  Keys are ordered to match
     * their values, which be be obtained by calling <code>getValues()</code>.
     *
     * As an optimization, the Vector returned is the instance member 
     * of this class.  Please don't modify outside the scope of this class.
     */
    Vector getKeys() {
        return this._keys;
    }


    /**
     * Obtain values contained in this buckets.  Values are ordered to match
     * their keys, which be be obtained by calling <code>getKeys()</code>.
     *
     * As an optimization, the Vector returned is the instance member 
     * of this class.  Please don't modify outside the scope of this class.
     */
    Vector getValues() {
        return this._values;
    }


    /**
     * Implement Externalizable interface.
     */
    public void writeExternal(ObjectOutput out) 
    throws IOException {
        out.writeInt(_depth);

        int entries = _keys.size();
        out.writeInt(entries);

        // write keys
        for (int i=0; i<entries; i++) {
            out.writeObject(_keys.elementAt(i));
        }
        // write values
        for (int i=0; i<entries; i++) {
            out.writeObject(_values.elementAt(i));
        }
    }
    
    
    /**
     * Implement Externalizable interface.
     */
    public void readExternal(ObjectInput in) 
    throws IOException, ClassNotFoundException {
        _depth = in.readInt();

        int entries = in.readInt();

        // prepare vectors
        int vectorSize = Math.max(entries, OVERFLOW_SIZE);
        _keys = new Vector(vectorSize);
        _values = new Vector(vectorSize);

        // read keys
        for (int i=0; i<entries; i++) {
            _keys.addElement(in.readObject());
        }
        // read values
        for (int i=0; i<entries; i++) {
            _values.addElement(in.readObject());
        }
    }

    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("HashBucket {depth=");
        buf.append(_depth);
        buf.append(", keys=");
        buf.append(_keys);
        buf.append(", values=");
        buf.append(_values);
        buf.append("}");
        return buf.toString();
    }
}
