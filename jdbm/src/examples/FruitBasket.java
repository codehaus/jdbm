
import jdbm.JDBMEnumeration;
import jdbm.JDBMHashtable;
import jdbm.JDBMRecordManager;
import java.io.IOException;

/**
 * Sample JDBM application to demonstrate the use of basic JDBM operations
 */
public class FruitBasket {
    JDBMRecordManager recman;
    JDBMHashtable hashtable;

    public FruitBasket() {
        try {
            // open persistent hashtable
            recman = new JDBMRecordManager("fruits");
            hashtable = recman.getHashtable("basket");

            // insert keys and values
            hashtable.put("bananas", "yellow");
            hashtable.put("strawberries", "red");
            hashtable.put("kiwis", "green");

            // Display content of fruit basket
            System.out.print("Fruit basket contains: ");
            JDBMEnumeration enum = hashtable.keys();
            while (enum.hasMoreElements()) {
                String fruit = (String)enum.nextElement();
                System.out.print(" "+fruit);
            }
            System.out.println();


            System.out.println("Fruit colors:");

            // display color of a specific fruit
            String bananasColor = (String)hashtable.get("bananas");
            System.out.println("bananas are "+bananasColor);

            // remove a specific fruit from hashtable
            hashtable.remove("bananas");

            // iterate over remaining objects
            enum = hashtable.keys();
            while (enum.hasMoreElements()) {
                String fruit = (String)enum.nextElement();
                String color = (String)hashtable.get(fruit);
                System.out.println(fruit+" are "+color);
            }

            // cleanup
            hashtable.dispose();
            recman.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new FruitBasket();
    }
}
