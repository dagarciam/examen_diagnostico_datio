import com.minsait.ttaa.exercises.{FileReader, FileStrategy}
import org.scalatest.funsuite.AnyFunSuite

class FileStrategyTest extends AnyFunSuite {

  test("The file has an Unknown format [csv]"){
    assertThrows[RuntimeException]{
      val readCsv = new FileReader(FileStrategy("test.dat"))
    }
  }
  test("The file doesn't exist, getting a None"){
    val file = "test.csv"
    val readCsv = new FileReader(FileStrategy(file))
    assert(readCsv.read(file) === None)
  }

}
