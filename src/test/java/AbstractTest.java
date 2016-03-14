package test.java;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;

public abstract class AbstractTest {

  protected String url1;
  protected String pass2;
  protected String pass1;
  protected String user1;
  protected String url2;
  protected String user2;
  protected Connection con1;
  protected Connection con2;

  @BeforeClass
  public void init() {
    url1 = AbstractSuite.getUrl1();
    url2 = AbstractSuite.getUrl2();
    user1 = AbstractSuite.getUser1();
    user2 = AbstractSuite.getUser2();
    pass1 = AbstractSuite.getPass1();
    pass2 = AbstractSuite.getPass2();
    con1 = AbstractSuite.getCon1();
    con2 = AbstractSuite.getCon2();
  }

  protected void checkResult(Statement stm, String query, String[] strings) throws SQLException {
    ResultSet rs = stm.executeQuery(query);
    for (int i = 0; i < strings.length; i++) {
      Assert.assertTrue(rs.next());
      Assert.assertEquals(rs.getString(1), strings[i]);
    }
    Assert.assertFalse(rs.next(), "unexpected value: '" + rs.getString(1) + "'");
  }

}
