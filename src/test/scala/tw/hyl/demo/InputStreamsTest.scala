package tw.hyl.demo

import org.scalatest.testng.TestNGSuite
import java.io.{File, FileInputStream}
import scala.concurrent.Await
import scala.io.Source
import org.testng.annotations.{Test, DataProvider}
import scala.concurrent.duration._

/**
 *
 */
class InputStreamsTest extends TestNGSuite {

  @DataProvider(name = "positive")
  def dataSet: Array[Array[Any]] = {
    return Array(
      Array(
        "InputStreamsTest/2000000.csv", 2000000
      )
    )
  }

  @Test(dataProvider = "positive")
  def testSort(inputFileName: String, lines: Int) {
    val inputStream = new FileInputStream(new File("src/test/resources/%s".format(inputFileName)))

    val start = System.currentTimeMillis()
    val output = Await.result(InputStreams.sort(inputStream, 100000), 1.minutes)
    val elapse = System.currentTimeMillis() - start

    println(s"sorted: ${output.getAbsolutePath}")
    println(s"time: ${elapse / 1000} seconds")

    assert(Source.fromFile(output).getLines().length === lines)
  }

}


