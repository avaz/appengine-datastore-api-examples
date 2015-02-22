package com.avaz.appengine;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import static com.google.appengine.api.datastore.FetchOptions.Builder.withLimit;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PropertyProjection;
import static com.google.appengine.api.datastore.Query.*;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import static org.hamcrest.CoreMatchers.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Anderson
 */
public class DatastoreQueryExample {

  private LocalServiceTestHelper helper = new LocalServiceTestHelper(
          new LocalDatastoreServiceTestConfig().setDefaultHighRepJobPolicyUnappliedJobPercentage( 0.1f ) );

  @Before
  public void setUp() {
    helper.setUp();
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();

    Entity entity = new Entity( "Customer" );
    entity.setProperty( "firstName", "Jack" );
    entity.setProperty( "lastName", "Bauer" );
    entity.setUnindexedProperty( "age", 40 );
    ds.put( entity );

    entity = new Entity( "Customer" );
    entity.setProperty( "firstName", "Jack" );
    entity.setProperty( "lastName", "Stripator" );
    entity.setUnindexedProperty( "age", 140 );
    ds.put( entity );

    entity = new Entity( "Customer" );
    entity.setProperty( "firstName", "Jack" );
    entity.setProperty( "lastName", "Johnson" );
    entity.setUnindexedProperty( "age", 35 );
    ds.put( entity );

    entity = new Entity( "Customer" );
    entity.setProperty( "firstName", "Jack" );
    entity.setProperty( "lastName", "Brown" );
    entity.setUnindexedProperty( "age", 55 );
    ds.put( entity );

    entity = new Entity( "Customer" );
    entity.setProperty( "firstName", "Jason" );
    entity.setProperty( "lastName", "Bourne" );
    entity.setUnindexedProperty( "age", 33 );
    ds.put( entity );

    entity = new Entity( "Customer" );
    entity.setProperty( "firstName", "Jason" );
    entity.setProperty( "lastName", "Sexta-Feira 13" );
    entity.setUnindexedProperty( "age", 555 );
    ds.put( entity );
  }

  @After
  public void tearDown() {
    helper.tearDown();
  }

  @Test
  public void testSingleFilter() {
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
    Filter filter = new FilterPredicate( "firstName", FilterOperator.EQUAL, "Jack" );
    Query query = new Query( "Customer" );
    query.setFilter( filter );
    assertEquals( 4, ds.prepare( query ).countEntities( withLimit( 10 ) ) );
  }

  @Test
  public void testCompositeFilter() {
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
    Filter firstName = new FilterPredicate( "firstName", FilterOperator.GREATER_THAN_OR_EQUAL, "Jack" );
    Filter lastName = new FilterPredicate( "lastName", FilterOperator.EQUAL, "Bauer" );
    Filter firstLastNameFilter = CompositeFilterOperator.and( firstName, lastName );

    Query query = new Query( "Customer" );
    query.setFilter( firstLastNameFilter );

    assertEquals( 1, ds.prepare( query ).countEntities( withLimit( 10 ) ) );

  }

  @Test
  public void testProjection() {
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();

    Query query = new Query( "Customer" );
    Filter firstName = new FilterPredicate( "firstName", FilterOperator.GREATER_THAN_OR_EQUAL, "Jack" );
    Filter lastName = new FilterPredicate( "lastName", FilterOperator.EQUAL, "Bauer" );
    Filter firstLastNameFilter = CompositeFilterOperator.and( firstName, lastName );
    query.setFilter( firstLastNameFilter );
    query.addProjection( new PropertyProjection( "firstName", String.class ) );
    final Entity customer = ds.prepare( query ).asSingleEntity();

    assertEquals( "Jack", customer.getProperty( "firstName" ) );
    assertThat( customer.getProperty( "lastName" ), is( nullValue() ) );
  }

  @Test( expected = IllegalArgumentException.class )
  public void testCompositeInequalityFilter() {
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
    Filter firstName = new FilterPredicate( "firstName", FilterOperator.GREATER_THAN_OR_EQUAL, "Jack" );
    Filter lastName = new FilterPredicate( "lastName", FilterOperator.LESS_THAN_OR_EQUAL, "Bauer" );
    Filter firstLastNameFilter = CompositeFilterOperator.and( firstName, lastName );

    Query query = new Query( "Customer" );
    query.setFilter( firstLastNameFilter );
    ds.prepare( query ).countEntities( withLimit( 10 ) );
  }
  /*
   * Para query em diferentes campos funcionar é necessário criar um CompositeIndex:
   - kind: Customer
   properties:
   - name: firstName
   direction: asc
   - name: lastName
   direction: desc
   */

  @Test( expected = IllegalArgumentException.class )
  public void testUnindexedFilter() {
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
    Filter firstName = new FilterPredicate( "firstName", FilterOperator.GREATER_THAN_OR_EQUAL, "Jack" );
    Filter lastName = new FilterPredicate( "lastName", FilterOperator.LESS_THAN_OR_EQUAL, "Bauer" );
    Filter firstLastNameFilter = CompositeFilterOperator.and( firstName, lastName );

    Query query = new Query( "Customer" ).setFilter( new FilterPredicate( "age", FilterOperator.GREATER_THAN, 55 ) );
    query.setFilter( firstLastNameFilter );

    ds.prepare( query ).countEntities( withLimit( 10 ) );
  }

  @Test
  public void testKeys() throws EntityNotFoundException {
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
    Key guestbookKey = KeyFactory.createKey( "Guestbook", "mybook" );
    Entity parent = new Entity( guestbookKey );
    parent.setProperty( "name", "parent" );
    ds.put( parent );
    Entity entity = new Entity( "Guestbook", guestbookKey );
    ds.put( entity );
    parent = ds.get( guestbookKey );
    
    assertThat( parent, is( not( nullValue() ) ) );
    assertEquals( "parent", parent.getProperty( "name" ) );
    assertEquals( guestbookKey, parent.getKey() );
  }

}
