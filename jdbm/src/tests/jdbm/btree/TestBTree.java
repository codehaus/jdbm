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

package jdbm.btree;

import jdbm.JDBMEnumeration;
import jdbm.recman.RecordManager;
import jdbm.recman.TestRecordFile;
import jdbm.helper.MRU;
import jdbm.helper.ObjectCache;
import jdbm.helper.StringComparator;
import jdbm.helper.TupleBrowser;
import jdbm.helper.Tuple;

import junit.framework.*;

import java.io.IOException;
import java.io.Serializable;

import java.util.Set;
import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *  This class contains all Unit tests for {@link BTree}.
 *
 *  @author <a href="mailto:boisvert@exoffice.com">Alex Boisvert</a>
 *  @version $Id: TestBTree.java,v 1.2 2001/09/25 06:24:54 boisvert Exp $
 */
public class TestBTree extends TestCase {

  static final boolean DEBUG = true;
      // the number of threads to be started in the synchronization test
  static final int THREAD_NUMBER = 5;
      // the size of the content of the maps for the synchronization
      // test. Beware that THREAD_NUMBER * THREAD_CONTENT_COUNT < Integer.MAX_VALUE.
  static final int THREAD_CONTENT_SIZE = 1000;
      // for how long should the threads run.
  static final int THREAD_RUNTIME = 1000;

  protected TestResult result_;

    public TestBTree( String name ) {
        super( name );
    }

    public void setUp() {
        TestRecordFile.deleteTestFile();
    }

    public void tearDown() {
        TestRecordFile.deleteTestFile();
    }


//----------------------------------------------------------------------
/**
 * Overrides TestCase.run(TestResult), so the errors from threads
 * started from this thread can be added to the testresult. This is
 * shown in
 * http://www.javaworld.com/javaworld/jw-12-2000/jw-1221-junit.html
 *
 * @param result the testresult
 */

  public void run(TestResult result)
  {
    result_ = result;
    super.run(result);
    result_ = null;
  }

//----------------------------------------------------------------------
/**
 * Handles the exceptions from other threads, so they are not ignored
 * in the junit test result. This method must be called from every
 * thread's run() method, if any throwables were throws.
 *
 * @param t the throwable (either from an assertEquals, assertTrue,
 * fail, ... method, or an uncaught exception to be added to the test
 * result of the junit test.
 */

  protected void handleThreadException(final Throwable t)
  {
    synchronized(result_)
    {
      if(t instanceof AssertionFailedError)
        result_.addFailure(this,(AssertionFailedError)t);
      else
        result_.addError(this,t);
    }
  }



     /**
      *  Basic tests
      */
     public void testBasics() throws IOException {
         if ( DEBUG ) {
             System.out.println("TestBTree.testBasics");
         }

         RecordManager recman = new RecordManager( "test" );
         ObjectCache cache = new ObjectCache( recman, new MRU( 100 ) );
         BTree tree = new BTree( recman, cache, new StringComparator() );

         tree.insert( "test1", "value1", false);
         tree.insert( "test2", "value2", false);

         String result;
         result = (String) tree.find( "test0" );
         if ( result != null ) {
             throw new Error( "Test0 shouldn't be found" );
         }

         result = (String) tree.find( "test1" );
         if ( result == null || !result.equals( "value1" ) ) {
             throw new Error( "Invalid value for test1: " + result );
         }

         result = (String) tree.find( "test2" );
         if ( result == null || !result.equals( "value2" ) ) {
             throw new Error( "Invalid value for test2: " + result );
         }

         result = (String) tree.find( "test3" );
         if ( result != null ) {
             throw new Error( "Test3 shouldn't be found" );
         }

         recman.close();

     }

     /**
      *  Basic tests, just use the simple test possibilities of junit (cdaller)
      */
     public void testBasics2() throws IOException {
         if ( DEBUG )
             System.out.println("TestBTree.testBasics2");

         RecordManager recman = new RecordManager( "test" );
         ObjectCache cache = new ObjectCache( recman, new MRU( 100 ) );
         BTree tree = new BTree( recman, cache, new StringComparator() );

         tree.insert( "test1", "value1", false);
         tree.insert( "test2", "value2", false);

         assertEquals(null,tree.find("test0"));
         assertEquals("value1",tree.find("test1"));
         assertEquals("value2",tree.find("test2"));
         assertEquals(null,tree.find("test3"));

         recman.close();

     }


     /**
      *  Test what happens after the recmanager has been closed but the
      *  btree is accessed. WHAT SHOULD HAPPEN???????????
      * (cdaller)
      */
   public void testClose()  throws IOException {
         if ( DEBUG )
             System.out.println("TestBTree.testClose");

         RecordManager recman = new RecordManager( "test" );
         ObjectCache cache = new ObjectCache( recman, new MRU( 100 ) );
         BTree tree = new BTree( recman, cache, new StringComparator() );

         tree.insert( "test1", "value1", false);
         tree.insert( "test2", "value2", false);

         assertEquals(null,tree.find("test0"));
         assertEquals("value1",tree.find("test1"));
         assertEquals("value2",tree.find("test2"));
         assertEquals(null,tree.find("test3"));

         recman.close();

         try
         {
           tree.browse();
           fail("Should throw an IOException on access on not opened btree");
         }
         catch(IOException e)
         {
         }
         try
         {
           tree.find(new Integer(0));
           fail("Should throw an IOException on access on not opened btree");
         }
         catch(IOException e)
         {
         }
         try
         {
           tree.findGreaterOrEqual(new Integer(0));
           fail("Should throw an IOException on access on not opened btree");
         }
         catch(IOException e)
         {
         }
         try
         {
           tree.insert("test5", "value5",false);
           fail("Should throw an IOException on access on not opened btree");
         }
         catch(IOException e)
         {
         }
         try
         {
           tree.remove("test0");
           fail("Should throw an IOException on access on not opened btree");
         }
         catch(IOException e)
         {
         }
 //          try
 //          {
 //            tree.size();
 //            fail("Should throw an IOException on access on not opened btree");
 //          }
 //          catch(IOException e)
 //          {
 //          }
     }


     /**
      *  Test to insert different objects into one btree. (cdaller)
      */
   public void testInsert()  throws IOException {
         if ( DEBUG )
             System.out.println("TestBTree.testInsert");
         RecordManager recman = new RecordManager( "test" );
         ObjectCache cache = new ObjectCache( recman, new MRU( 100 ) );
         BTree tree = new BTree( recman, cache, new StringComparator() );

             // insert differnt objects and retrieve them
         tree.insert("test1", "value1",false);
         tree.insert("test2","value2",false);
         tree.insert("one", new Integer(1),false);
         tree.insert("two",new Long(2),false);
         tree.insert("myownobject",new TestObject(new Integer(234)),false);

         assertEquals("value2",(String)tree.find("test2"));
         assertEquals("value1",(String)tree.find("test1"));
         assertEquals(new Integer(1),(Integer)tree.find("one"));
         assertEquals(new Long(2),(Long)tree.find("two"));

             // what happens here? must not be replaced, does it return anything?
             // probably yes!
         assertEquals("value1",tree.insert("test1","value11",false));
         assertEquals("value1",tree.find("test1")); // still the old value?
         assertEquals("value1",tree.insert("test1","value11",true));
         assertEquals("value11",tree.find("test1")); // now the new value!

         TestObject expected_obj = new TestObject(new Integer(234));
         TestObject btree_obj = (TestObject)tree.find("myownobject");
         assertEquals(expected_obj, btree_obj);

         recman.close();
   }


     /**
      *  Test to remove  objects from the btree. (cdaller)
      */
   public void testRemove()  throws IOException {
         if ( DEBUG )
             System.out.println("TestBTree.testRemove");

         RecordManager recman = new RecordManager( "test" );
         ObjectCache cache = new ObjectCache( recman, new MRU( 100 ) );
         BTree tree = new BTree( recman, cache, new StringComparator() );

         tree.insert("test1", "value1",false);
         tree.insert("test2","value2",false);
         assertEquals("value1",(String)tree.find("test1"));
         assertEquals("value2",(String)tree.find("test2"));
         tree.remove("test1");
         assertEquals(null,(String)tree.find("test1"));
         assertEquals("value2",(String)tree.find("test2"));
         tree.remove("test2");
         assertEquals(null,(String)tree.find("test2"));

         int iterations = 1000;

         for(int count = 0; count < iterations; count++)
           tree.insert("num"+count,new Integer(count),false);

         assertEquals(iterations,tree.size());

         for(int count = 0; count < iterations; count++)
           assertEquals(new Integer(count),tree.find("num"+count));

         for(int count = 0; count < iterations; count++) {
           tree.remove("num"+count);
         }

 //        for(int count = iterations-1; count >= 0; count--)
 //          tree.remove("num"+count);

         assertEquals(0,tree.size());

         recman.close();
   }

     /**
      *  Test to find differents objects in the btree. (cdaller)
      */
   public void testFind()  throws IOException {
         if ( DEBUG )
             System.out.println("TestBTree.testFind");
         RecordManager recman = new RecordManager( "test" );
         ObjectCache cache = new ObjectCache( recman, new MRU( 100 ) );
         BTree tree = new BTree( recman, cache, new StringComparator() );

         tree.insert("test1", "value1",false);
         tree.insert("test2","value2",false);

         Object value = tree.find("test1");
         assertTrue(value instanceof String);
         assertEquals("value1",value);

         tree.insert("","Empty String as key",false);
         assertEquals("Empty String as key",(String)tree.find(""));

         assertEquals(null,(String)tree.find("someoneelse"));


         recman.close();
   }


       /**
      *  Test to insert, retrieve and remove a large amount of data. (cdaller)
      */
   public void testLargeDataAmount()  throws IOException {
         if ( DEBUG )
             System.out.println("TestBTree.testLargeDataAmount");
         RecordManager recman = new RecordManager( "test" );
         ObjectCache cache = new ObjectCache( recman, new MRU( 100 ) );
         BTree tree = new BTree( recman, cache, new StringComparator() );

         int iterations = 10000;

           // insert data
         for(int count = 0; count < iterations; count++)
         {
           assertEquals(null,tree.insert("num"+count,new Integer(count),false));
         }

           // find data
         for(int count = 0; count < iterations; count++)
         {
           assertEquals(new Integer(count), tree.find("num"+count));
         }

             // delete data
         for(int count = 0; count < iterations; count++)
         {
           assertEquals(new Integer(count),tree.remove("num"+count));
         }

         assertEquals(0,tree.size());

         recman.close();
   }

/**
 * Test access from multiple threads. Assertions only work, when the
 * run() method is overridden and the exceptions of the threads are
 * added to the resultset of the TestCase. see run() and
 * handleException().
 */
  public void testMultithreadAccess()
    throws IOException
  {
    if ( DEBUG )
      System.out.println("TestBTree.testMultithreadAccess");

    RecordManager recman = new RecordManager( "test" );
    ObjectCache cache = new ObjectCache( recman, new MRU( 100 ) );
    BTree tree = new BTree( recman, cache, new StringComparator() );

    TestThread[] thread_pool = new TestThread[THREAD_NUMBER];
    String name;
    Map content;

        // create content for the tree, different content for different threads!
    for(int thread_count = 0; thread_count < THREAD_NUMBER; thread_count++) {
      name = "thread"+thread_count;
      content = new TreeMap();
      for(int content_count = 0; content_count < THREAD_CONTENT_SIZE; content_count++) {
            // guarantee, that keys and values do not overleap,
            // otherwise one thread removes some keys/values of
            // other threads!
        content.put(name+"_"+content_count,new
Integer(thread_count*THREAD_CONTENT_SIZE+content_count));
      }
      thread_pool[thread_count] = new TestThread(name,tree,content);
      thread_pool[thread_count].start();
    }

    try {
      Thread.sleep(THREAD_RUNTIME);
    }
    catch(InterruptedException ignore) {}
        // stop threads:
    for(int thread_count = 0; thread_count < THREAD_NUMBER; thread_count++) {
      thread_pool[thread_count].setStop();
    }
        // wait until the threads really stop:
    try {
      for(int thread_count = 0; thread_count < THREAD_NUMBER; thread_count++)
        thread_pool[thread_count].join();
    }
    catch(InterruptedException ignore) {}
    recman.close();
  }


    /**
     *  Helper method to 'simulate' the methods of an entry set of the btree.
     */
  protected static boolean containsKey(Object key, BTree btree)
    throws IOException
  {
    return(btree.find(key) != null);
  }

    /**
     *  Helper method to 'simulate' the methods of an entry set of the btree.
     */
  protected static boolean containsValue(Object value, BTree btree)
    throws IOException
  {
    TupleBrowser browser = btree.browse();
    Tuple tuple = new Tuple();
    while(browser.getNext(tuple))
    {
      if(tuple.getValue().equals(value))
        return(true);
    }
//    System.out.println("Comparation of '"+value+"' with '"+ tuple.getValue()+"' FAILED");
    return(false);
  }

    /**
     *  Helper method to 'simulate' the methods of an entry set of the btree.
     */
  protected static boolean contains(Map.Entry entry, BTree btree)
  throws IOException
  {
    Object tree_obj = btree.find(entry.getKey());
    if(tree_obj == null)
      return(entry.getValue() == null);  // can't distuingish, if value is null or not found!!!!!!

    return(tree_obj.equals(entry.getValue()));
  }

      /**
     *  Runs all tests in this class
     */
    public static void main( String[] args ) {
        junit.textui.TestRunner.run( new TestSuite( TestBTree.class ) );
    }

//----------------------------------------------------------------------

/**
 * Inner class for testing puroposes only (multithreaded access)
 */
class TestThread
    extends Thread
{
    Map _content;
    BTree _btree;
    boolean _continue = true;
    int THREAD_SLEEP_TIME = 50; // in ms
    String _name;

    TestThread( String name, BTree btree, Map content )
    {
        _content = content;
        _btree = btree;
        _name = name;
    }

    public void setStop()
    {
        _continue = false;
    }

    private void action()
        throws IOException
    {
        Iterator iterator = _content.entrySet().iterator();
        Map.Entry entry;
        if ( DEBUG ) {
            System.out.println("Thread "+_name+": fill btree.");
        }
        while( iterator.hasNext() ) {
            entry = (Map.Entry) iterator.next();
            assertEquals( null, _btree.insert( entry.getKey(), entry.getValue(), false ) );
        }

        // as other threads are filling the btree as well, the size
        // of the btree is unknown (but must be at least the size of
        // the content map)
        assertTrue( _content.size() <= _btree.size() );

        iterator = _content.entrySet().iterator();
        if ( DEBUG ) {
            System.out.println( "Thread " + _name + ": iterates btree." );
        }
        while( iterator.hasNext() ) {
            entry = (Map.Entry) iterator.next();
            assertEquals( entry.getValue(), _btree.find( entry.getKey() ) );
            assertTrue( contains( entry, _btree ) );
            assertTrue( containsKey( entry.getKey(), _btree ) );
            assertTrue( containsValue( entry.getValue(), _btree ) );
        }

        iterator = _content.entrySet().iterator();
        Object key;
        if ( DEBUG ) {
            System.out.println( "Thread " + _name + ": removes his elements from the btree." );
        }
        while( iterator.hasNext() ) {
            key = ( (Map.Entry) iterator.next() ).getKey();
            _btree.remove( key );
            assertTrue( ! containsKey( key,_btree ) );
        }
    }

    public void run()
    {
      if(DEBUG)
        System.out.println("Thread "+_name+": started.");
      try
      {
        while(_continue)
        {
          action();
          try
          {
            Thread.sleep(THREAD_SLEEP_TIME);
          }
          catch(InterruptedException ie)
          {
          }
        }
      }
      catch(Throwable t)
      {
        if(DEBUG)
        {
          System.err.println("Thread "+_name+" threw an exception:");
          t.printStackTrace();
        }
        handleThreadException(t);
      }
      if(DEBUG)
        System.out.println("Thread "+_name+": stopped.");
    }
  } // end of class TestThread
}


/**
 * class for testing puroposes only (store as value in btree) not
 * implemented as inner class, as this prevents Serialization if
 * outer class is not Serializable.
 */
class TestObject
    implements Serializable
{

    Object _content;

    private TestObject()
    {
        // empty
    }


    public TestObject( Object content )
    {
        _content = content;
    }


    Object getContent()
    {
        return _content;
    }


    public boolean equals( Object obj )
    {
        if ( ! ( obj instanceof TestObject ) ) {
            return false;
        }
        return _content.equals( ( (TestObject) obj ).getContent() );
    }

    public String toString()
    {
        return( "TestObject {content='" + _content + "'}" );
    }

} // TestObject

