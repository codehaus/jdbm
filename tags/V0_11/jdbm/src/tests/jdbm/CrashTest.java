
package jdbm;

import jdbm.JDBMEnumeration;
import jdbm.JDBMHashtable;
import jdbm.recman.RecordManager;
import jdbm.hash.HTree;
import jdbm.helper.ObjectCache;
import jdbm.helper.MRU;
import java.io.IOException;

/**
 * Sample JDBM application to demonstrate the use of basic JDBM operations
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: CrashTest.java,v 1.4 2001/05/19 14:20:46 boisvert Exp $
 */
public class CrashTest {
    RecordManager recman;
    JDBMHashtable hashtable;

    public CrashTest() {
        try {
            // open persistent hashtable
            recman = new RecordManager("crashtest");

            ObjectCache cache = new ObjectCache(recman, new MRU(1));

            long root_recid = recman.getNamedObject("crash");
            if (root_recid == 0) {
                // create a new one
                root_recid = HTree.createRootDirectory(recman);
                recman.setNamedObject("crash", root_recid);
                recman.commit();
            }
            hashtable = new HTree(recman, cache, root_recid);
            // hashtable = recman.getHashtable("crash");

            checkConsistency();

            while (true) {
                Integer countInt = (Integer)hashtable.get("count");
                if (countInt == null) {
                    System.out.println("Create new crash test");
                    countInt = new Integer(0);
                }
                int count = countInt.intValue();

                System.out.print(","+count);
                System.out.flush();

                int mod = count % 2;
                int delete_window = 20;
                int update_window = 10;

                if ((mod) == 0) {
                    // create some entries
                    for (int i=0; i<10; i++) {
                        String id = " "+count+"-"+i;
                        hashtable.put("key"+id, "value"+id);
                    }

                    // delete some entries
                    if (count > delete_window) {
                        for (int i=0; i<10; i++) {
                            String id = " "+(count-delete_window)+"-"+i;
                            hashtable.remove("key"+id);
                        }
                    }
                } else if ((mod) == 1) {
                    if (count > update_window+1) {
                        // update some stuff
                        for (int i=0; i<5; i++) {
                            String id = " "+(count-update_window+1)+"-"+i;
                            String s = (String)hashtable.get("key"+id);
                            if ((s == null) || !s.equals("value"+id)) {
                                throw new Error("Invalid value.  Expected: "
                                                +("value"+id)
                                                +", got: "
                                                +s);
                            }
                            hashtable.put("key"+id, s+"-updated");
                        }
                    }
                }

                hashtable.put("count", new Integer(count+1));
                recman.commit();

                count++;
            }

            // BTW:  There is no cleanup.  It's a crash test after all.

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    void checkConsistency() {
        // TODO
    }

    public static void main(String[] args) {
        System.out.print("Please try to stop me anytime. ");
        System.out.println("CTRL-C, kill -9, anything goes!.");
        new CrashTest();
    }
}
