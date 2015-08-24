package as.leap.monqo.jobs

import org.junit.{After, Before, Test}

/**
 * class description
 * <p>add some remarks.</p>
 * author: jeff.tsai
 * date: 2015-08-13
 * email: jjeffcaii@outlook.com
 */
class SimpleMongoSyncTest {

  @Before
  def prepare(): Unit = {
    System.setProperty("spark.master", "local[4]")
  }

  @Test
  def test(): Unit = {
    val args = Array[String]("-d", "sexyimg", "-t", "images")
    SimpleMongoSync.main(args)
  }

  @After
  def cleanup(): Unit = {
    //TODO
  }
}
