package org.kiji.cassandra;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.dataset.DataSet;
import org.junit.Test;
import org.junit.Rule;


public class TestApp {

  @Rule
  public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("simple.cql", "mykeyspace"));

  @Test
  public void myTest() throws Exception {
    App myApp = new App();
    myApp.setup(cassandraCQLUnit.session);

  }

}