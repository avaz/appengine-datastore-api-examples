package com.avaz.appengine;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import static com.google.appengine.api.datastore.FetchOptions.Builder.withLimit;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Anderson
 */
public class DatastoreConsitencyExample {

  private LocalServiceTestHelper helper = new LocalServiceTestHelper(
          new LocalDatastoreServiceTestConfig().setDefaultHighRepJobPolicyUnappliedJobPercentage( 0.1f ) );

  @Before
  public void setUp() {
    helper.setUp();
  }

  @After
  public void tearDown() {
    helper.tearDown();
  }


  @Test()
  public void eventualConsistency() {
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
    Key agentKey = KeyFactory.createKey( "Agents", "killers");
    
    Entity entity = new Entity( "Customer", agentKey );
    entity.setProperty( "firstName", "Jack" );
    entity.setProperty( "lastName", "Bauer" );
    entity.setUnindexedProperty( "age", 40 );
    ds.put( entity );

    entity = new Entity( "Customer", agentKey );
    entity.setProperty( "firstName", "Jason" );
    entity.setProperty( "lastName", "Bourne" );
    entity.setUnindexedProperty( "age", 33 );
    ds.put( entity );

    Query.Filter filter = new Query.FilterPredicate( "firstName", Query.FilterOperator.EQUAL, "Jack" );
    Query query = new Query( "Customer" );
    query.setFilter( filter );
    assertEquals( 0, ds.prepare( query ).countEntities( withLimit( 10 ) ) );

  }
  
  @Test
  public void strongConsistency() {
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
    Key agentKey = KeyFactory.createKey( "Agents", "killers");
    
    Entity entity = new Entity( "Customer", agentKey );
    entity.setProperty( "firstName", "Jack" );
    entity.setProperty( "lastName", "Bauer" );
    entity.setUnindexedProperty( "age", 40 );
    ds.put( entity );

    entity = new Entity( "Customer", agentKey );
    entity.setProperty( "firstName", "Jason" );
    entity.setProperty( "lastName", "Bourne" );
    entity.setUnindexedProperty( "age", 33 );
    ds.put( entity );

    Query.Filter filter = new Query.FilterPredicate( "firstName", Query.FilterOperator.EQUAL, "Jack" );
    Query query = new Query( "Customer" ).setAncestor( agentKey );
    query.setFilter( filter );
    assertEquals( 1, ds.prepare( query ).countEntities( withLimit( 10 ) ) );
  }
}
