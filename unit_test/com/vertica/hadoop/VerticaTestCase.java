package com.vertica.hadoop;

import junit.framework.TestCase;

public class VerticaTestCase extends TestCase {
  public VerticaTestCase(String name) {
    super(name);
  }

  {
    AllTests.configure();
  }
}
