package as.leap.monqo.jobs

import org.junit.{After, Before, Test}

/**
 * class description
 * <p>add some remarks.</p>
 * author: jeff.tsai
 * date: 2015-08-12
 * email: jjeffcaii@outlook.com
 */
class HBase2ParquetTest {


  @Before
  def prepare(): Unit = {
    System.setProperty("spark.master", "local[4]")
  }

  @Test
  def test(): Unit = {
    HBase2Parquet.main(Array.empty[String])
  }

  @After
  def cleanup(): Unit = {

  }

}
