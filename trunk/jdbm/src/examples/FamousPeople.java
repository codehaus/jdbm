
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;
import jdbm.helper.MRU;
import jdbm.helper.ObjectCache;
import jdbm.helper.StringComparator;
import jdbm.recman.RecordManager;
import jdbm.btree.BTree;

import java.io.IOException;

/**
 *  Famous People example.
 *  <p>
 *  Demonstrates the use of B+Tree data structure to manage a list of
 *  people and their occupation.  The example covers insertion,
 *  ordered traversal, reverse traversal and range lookup of records.
 *
 *  @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 *  @version $Id: FamousPeople.java,v 1.2 2001/06/02 14:37:02 boisvert Exp $
 */
public class FamousPeople {

    static String DATABASE = "people";

    static String BTREE_NAME = "FamousPeople";

    static String[] people =
        { "Greenspan, Alan",
          "Williams-Byrd, Julie",
          "Picasso, Pablo",
          "Stallman, Richard",
          "Fort, Paul",
          "Søndergaard, Ole",
          "Schwarzenegger, Arnold",
          "Dulkinys, Susanna" };

    static String[] occupations =
        { "Federal Reserve Board Chairman",
          "Engineer",
          "Painter",
          "Programmer",
          "Poet",
          "Typographer",
          "Actor",
          "Designer" };

    static String PREFIX = "S";


    /**
     * Example main entrypoint.
     */
    public static void main( String[] args ) {
        RecordManager recman;
        ObjectCache   cache;
        BTree         tree;
        long          recid;
        Tuple         tuple = new Tuple();
        TupleBrowser  browser;

        try {
            // open database and setup an object cache
            recman = new RecordManager( DATABASE );
            cache = new ObjectCache( recman, new MRU( 100 ) );

            // try to reload an existing B+Tree
            recid = recman.getNamedObject( BTREE_NAME );
            if ( recid != 0 ) {
                tree = BTree.load( recman, cache, recid );
                System.out.println( "Reloaded existing BTree with " + tree.size()
                                    + " famous people." );
            } else {
                // create a new B+Tree data structure and use a StringComparator
                // to order the records based on people's name.
                tree = new BTree( recman, cache, new StringComparator() );
                recman.setNamedObject( BTREE_NAME, tree.getRecid() );
                System.out.println( "Created a new empty BTree" );
            }

            // insert people with their respective occupation
            System.out.println();
            for ( int i=0; i<people.length; i++ ) {
                System.out.println( "Insert: " + people[i] );
                tree.insert( people[ i ], occupations[ i ], false );
            }

            // make the data persistent in the database
            recman.commit();

            // show list of people with their occupation
            System.out.println();
            System.out.println( "Person                   Occupation       " );
            System.out.println( "------------------       ------------------" );

            // traverse people in order
            browser = tree.browse();
            while ( browser.getNext( tuple ) ) {
                print( tuple );
            }

            // traverse people in reverse order
            System.out.println();
            System.out.println( "Reverse order:" );
            browser = tree.browse( null ); // position browser at end of the list
            while ( browser.getPrevious( tuple ) ) {
                print( tuple );
            }

            // display people whose name start with PREFIX range
            System.out.println();
            System.out.println( "All people whose name start with '" + PREFIX + "':" );
            browser = tree.browse( PREFIX );
            while ( browser.getNext( tuple ) ) {
                String key = (String) tuple.getKey();
                if ( key.startsWith( PREFIX ) ) {
                    print( tuple );
                } else {
                    break;
                }
            }

        } catch ( Exception except ) {
            except.printStackTrace();
        }
    }


    /**
     * Print a Tuple containing a ( Person, Occupation ) pair.
     */
    static void print( Tuple tuple ) {
        String person = (String) tuple.getKey();
        String occupation = (String) tuple.getValue();
        System.out.println( pad( person, 25) + occupation );
    }


    /**
     * Pad a string with spaces on the right.
     *
     * @param str String to add spaces
     * @param width Width of string after padding
     */
    static String pad( String str, int width ) {
        StringBuffer buf = new StringBuffer( str );
        int space = width-buf.length();
        while ( space-- > 0 ) {
            buf.append( ' ' );
        }
        return buf.toString();
    }
}
