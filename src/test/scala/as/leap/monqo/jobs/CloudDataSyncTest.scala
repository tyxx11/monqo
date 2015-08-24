package as.leap.monqo.jobs

import org.junit.{Before, Test}

/**
 * class description
 * <p>add some remarks.</p>
 * author: jeff.tsai
 * date: 2015-08-14
 * email: jjeffcaii@outlook.com
 */
class CloudDataSyncTest {

  @Before
  def prepare(): Unit = {
    System.setProperty("spark.master", "local[*]")
  }

  @Test
  def test(): Unit = {
    val args = Array("-i", "53d21c66e4b04663ccc7fbfd", "-c", "Playlist")
    CloudDataSync.main(args)
    val args2 = Array("-i", "53d21c66e4b04663ccc7fbfd", "-c", "TopTrack")
    CloudDataSync.main(args2)
  }

}
