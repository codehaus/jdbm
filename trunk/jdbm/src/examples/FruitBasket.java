
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.helper.FastIterator;
import jdbm.htree.HTree;

import java.io.IOException;
import java.util.Properties;

/**
 * Sample JDBM application to demonstrate the use of basic JDBM operations.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: FruitBasket.java,v 1.2 2002/05/31 06:35:36 boisvert Exp $
 */
public class FruitBasket
{
    RecordManager  recman;
    HTree          hashtable;
    FastIterator   iter;
    String         fruit;
    String         color;


    public FruitBasket()
        throws IOException
    {
        // create or open fruits record manager
        Properties props = new Properties();
        recman = RecordManagerFactory.createRecordManager( "fruits", props );

        // create or load fruit basket (hashtable of fruits)
        long recid = recman.getNamedObject( "basket" );
        if ( recid != 0 ) {
            hashtable = HTree.load( recman, recid );
        } else {
            hashtable = HTree.createInstance( recman );
            recman.setNamedObject( "basket", hashtable.getRecid() );
        }
    }


    public void runDemo()
        throws IOException
    {
        // insert keys and values
        hashtable.put( "bananas", "yellow" );
        hashtable.put( "strawberries", "red" );
        hashtable.put( "kiwis", "green" );

        // Display content of fruit basket
        System.out.print( "Fruit basket contains: " );
        iter = hashtable.keys();
        fruit = (String) iter.next();
        while ( fruit != null ) {
            System.out.print( " " + fruit );
            fruit = (String) iter.next();
        }
        System.out.println();


        System.out.println( "Fruit colors:" );

        // display color of a specific fruit
        String bananasColor = (String)hashtable.get( "bananas" );
        System.out.println( "bananas are " + bananasColor );

        // remove a specific fruit from hashtable
        hashtable.remove( "bananas" );

        // iterate over remaining objects
        iter = hashtable.keys();
        fruit = (String) iter.next();
        while ( fruit != null ) {
            color = (String) iter.next();
            System.out.println( fruit + " are " + color );
        }

        // cleanup
        recman.close();
    }


    public static void main( String[] args )
    {
        try {
            FruitBasket basket = new FruitBasket();
            basket.runDemo();
        } catch ( IOException ioe ) {
            ioe.printStackTrace();
        }
    }

}
