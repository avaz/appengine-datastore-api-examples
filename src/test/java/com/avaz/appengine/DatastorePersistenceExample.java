package com.avaz.appengine;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import static com.google.appengine.api.datastore.FetchOptions.Builder.withLimit;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

/**
 *
 * @author Anderson
 */
public class DatastorePersistenceExample {

  private LocalServiceTestHelper helper = new LocalServiceTestHelper( new LocalDatastoreServiceTestConfig() );

  @Before
  public void setUp() {
    helper.setUp();
  }

  @After
  public void tearDown() {
    helper.tearDown();
  }

  @Test
  public void testBasicPersistence() {
    DatastoreService ds = DatastoreServiceFactory
            .getDatastoreService();
    Entity entity = new Entity( "Customer" );
    entity.setProperty( "firstName", "Jack" );
    entity.setProperty( "lastName", "Bauer" );
    entity.setUnindexedProperty( "age", 40 );
    ds.put( entity );
    assertThat( entity.getKey(), is( not( nullValue() ) ) );
  }

  @Test
  public void testComplexPersistence() {
    DatastoreService ds = DatastoreServiceFactory
            .getDatastoreService();
    final Entity entity = new Entity( "Blog" );
    entity.setProperty( "content", "New Blog Post" );
    entity.setProperty( "comments", Arrays.asList( "First comment", "Second comment" ) );
    ds.put( entity );
    assertEquals( 1, ds.prepare( new Query( "Blog" ) ).countEntities( withLimit( 10 ) ) );
    List<String> comments = (List<String>) ds.prepare( new Query( "Blog" ) ).asSingleEntity().getProperty( "comments" );
    assertEquals( 2, comments.size() );
  }

}
