package org.kiji.cassandra;

import org.cassandraunit.AbstractCassandraUnit4TestCase;
import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.dataset.yaml.ClassPathYamlDataSet;
import org.junit.Test;


public class TestApp extends AbstractCassandraUnit4TestCase {

  @Override
  public DataSet getDataSet() {
    return new ClassPathYamlDataSet("data.yaml");
  }

  @Test
  public void myTest() throws Exception {
    String[] args = { "localhost"};
    App.main(args);
  }

}