
package jdbm;

import jdbm.JDBMEnumeration;
import jdbm.JDBMHashtable;
import jdbm.JDBMRecordManager;
import java.io.IOException;

import java.io.*;
import java.util.*;

/**
 * Test case provided by Daniel Herlemont to demonstrate a bug in
 * HashDirectory.  The returned Enumeration got into an infinite loop
 * on the same key/val pair.
 *
 * @version $Id: HashtableTest.java,v 1.3 2001/06/02 14:32:00 boisvert Exp $
 */
public class HashtableTest {

    JDBMRecordManager recman;
    JDBMHashtable hashtable;


    void populate() throws Exception {
        recman = new JDBMRecordManager(jdbmName);
        hashtable = recman.getHashtable(name);
        try {

            int max=1000;
            for (int i=0;i<max;i++) {
                String key="key"+i;
                String val="val"+i;
                hashtable.put(key,val);
                System.out.println("put key="+key+" val="+val);
            }

            System.out.println("populate completed");

        } finally {
            hashtable.dispose();
            recman.close();
        }

    }

    Object retrieve(Object key) throws Exception {
        recman = new JDBMRecordManager(jdbmName);
        hashtable = recman.getHashtable(name);
        try {
            Object val=hashtable.get(key);
            System.out.println("retrieve key="+key+" val="+val);
            return val;
        } finally {hashtable.dispose(); recman.close();  }
    }

    void enum() throws Exception {
        recman = new JDBMRecordManager(jdbmName);
        hashtable = recman.getHashtable(name);

        try {

            JDBMEnumeration enum=hashtable.keys();
            while (enum.hasMoreElements()) {
                Object key=enum.nextElement();
                Object val=hashtable.get(key);
                System.out.println("enum key="+key+" val="+val);
            }

        } finally {hashtable.dispose();    recman.close();  }
    }

    //-----------------------------------------------------------------------

    boolean enum=false;
    boolean populate=false;
    boolean retrieve=false;
    String jdbmName="hashtest";
    String name="hashtable";
    String onekey="onekey";

    void doCommands() throws Exception {

        if (enum) enum();
        if (populate) populate();
        if (retrieve) retrieve(onekey);

    }

    //-----------------------------------------------------------------------

    void getArgs(String args[]) throws Exception {

        for (int argn = 0; argn < args.length; argn++) {
            if (args[argn].equals("-enum")) {
                enum = true;
            } else if (args[argn].equals("-populate")) {
                populate = true;
            } else if (args[argn].equals("-retrieve")) {
                retrieve = true;
            } else if (args[argn].equals("-jdbmName") && argn < args.length - 1) {
                jdbmName = args[++argn];
            } else if (args[argn].equals("-key") && argn < args.length - 1) {
                onekey = args[++argn];
            }
            else if (args[argn].equals("-name") && argn < args.length - 1) {
                name = args[++argn];
            }
            else {
                System.err.println("unrecognized option : " + args[argn]);
                usage(System.err);
            }

        }
    }

    //-----------------------------------------------------------------------

    public void usage(PrintStream ps) {

        ps.println("Usage: java "+getClass().getName()+" Options");
        ps.println();
        ps.println("Options (with default values):");
        ps.println("-help print this");
    }


    //-----------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        System.setErr(System.out);
        HashtableTest instance = new HashtableTest();
        instance.getArgs(args);
        instance.doCommands();
    }

}

