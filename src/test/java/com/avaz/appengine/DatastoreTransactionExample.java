package com.avaz.appengine;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import static com.google.appengine.api.datastore.KeyFactory.createKey;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.TransactionOptions;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import java.util.Date;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Anderson
 */
public class DatastoreTransactionExample {

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

  @Test
  public void basicTransaction() throws EntityNotFoundException {
    DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
    Transaction txn = datastore.beginTransaction();
    try {
      Key employeeKey = KeyFactory.createKey( "Book", "Effective Java" );
      Entity employee = new Entity( employeeKey );
      employee.setProperty( "author", "Joshua Block" );
      datastore.put( employee );
      txn.commit();
    }
    finally {
      if ( txn.isActive() ) {
        txn.rollback();
      }
    }
  }

  @Test
  public void entityGroupTransaction() throws EntityNotFoundException {
    DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
    Transaction tx = datastore.beginTransaction();

    Entity vacationAlbum = new Entity( "Album", "vacation" );
    datastore.put( vacationAlbum );

    vacationAlbum.setProperty( "when", new Date() );
    datastore.put( vacationAlbum );
    Entity photo = new Entity( "Photo", vacationAlbum.getKey() );
    photo.setProperty( "photoUrl", "http://cool-photo1.jpg" );
    datastore.put( photo );
    photo = new Entity( "Photo", vacationAlbum.getKey() );
    photo.setProperty( "photoUrl", "http://cool-photo2.jpg" );
    datastore.put( photo );
    tx.commit();
    
    assertEquals( 3, datastore.prepare( new Query(vacationAlbum.getKey())).countEntities( FetchOptions.Builder.withLimit( 10 )));
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void differentGroupTransaction() throws EntityNotFoundException {
    DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
    Entity album = new Entity( "Album", "vacation" );
    datastore.put( album );
    
    Transaction tx = datastore.beginTransaction();

    album = datastore.get( album.getKey() );
    Entity photoNotAChild = new Entity( "Photo" );
    photoNotAChild.setProperty( "photoUrl", "http://photo.jpg" );
    datastore.put( photoNotAChild );
    
    tx.commit();

  }
  
  @Test
  public void crossGroupTransaction() throws EntityNotFoundException {
    DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
    TransactionOptions options = TransactionOptions.Builder.withXG(true);
    Transaction tx = datastore.beginTransaction(options);

    Key albumsKey = createKey( "Albums", "albums");
    Entity album = new Entity( "Album", albumsKey );
    datastore.put(album );
    album = new Entity( "Album", albumsKey );
    datastore.put( tx, album );

    Key photoStreamKey = createKey( "Stream", "photoStream");
    Entity photo = new Entity( "Photo", photoStreamKey );
    photo.setProperty( "photoUrl", "http://photo1.jpg" );
    datastore.put( tx, photo );
    photo = new Entity( "Photo", photoStreamKey );
    photo.setProperty( "photoUrl", "http://photo2.jpg" );
    datastore.put( tx, photo );
    photo = new Entity( "Photo", photoStreamKey );
    photo.setProperty( "photoUrl", "http://photo3.jpg" );
    datastore.put( tx, photo );
    photo = new Entity( "Photo", photoStreamKey );
    photo.setProperty( "photoUrl", "http://photo4.jpg" );
    datastore.put( tx, photo );
    
    tx.commit();

  }
  
  @Test(expected = IllegalArgumentException.class)
  public void crossGroupExceedingLimitTransaction() throws EntityNotFoundException {
    DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
    TransactionOptions options = TransactionOptions.Builder.withXG(true);
    Transaction tx = datastore.beginTransaction(options);

    Key albumsKey = createKey( "Albums", "albums");
    Entity album = new Entity( "Album", albumsKey );
    datastore.put( tx, album );

    Key photoStreamKey = createKey( "Stream", "photoStream1");
    Entity photo = new Entity( "Photo", photoStreamKey );
    photo.setProperty( "photoUrl", "http://photo1.jpg" );
    datastore.put( tx, photo );

    photoStreamKey = createKey( "Stream", "photoStream2");
    photo = new Entity( "Photo", photoStreamKey );
    photo.setProperty( "photoUrl", "http://photo2.jpg" );
    datastore.put( tx, photo );

    photoStreamKey = createKey( "Stream", "photoStream3");
    photo = new Entity( "Photo", photoStreamKey );
    photo.setProperty( "photoUrl", "http://photo3.jpg" );
    datastore.put( tx, photo );
    
    photoStreamKey = createKey( "Stream", "photoStream4");
    photo = new Entity( "Photo", photoStreamKey );
    photo.setProperty( "photoUrl", "http://photo4.jpg" );
    datastore.put( tx, photo );
    
    photoStreamKey = createKey( "Stream", "photoStream5");
    photo = new Entity( "Photo", photoStreamKey );
    photo.setProperty( "photoUrl", "http://photo5.jpg" );
    datastore.put( tx, photo );
    
    tx.commit();

  }
}
