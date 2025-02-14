package test.alipsa.bq

import com.google.cloud.bigquery.StandardSQLTypeName
import com.google.cloud.bigquery.Table
import org.junit.jupiter.api.Test
import se.alipsa.bq.BigQueryWrapper

class BigQueryWrapperTest {

  String datasetName = "issue_396015253"
  String tableName = "mtcars"

  Map createData() {
    def columns = [
        model: StandardSQLTypeName.STRING,
        mpg: StandardSQLTypeName.BIGNUMERIC,
        cyl: StandardSQLTypeName.INT64,
        disp: StandardSQLTypeName.BIGNUMERIC,
        hp: StandardSQLTypeName.BIGNUMERIC,
        drat: StandardSQLTypeName.BIGNUMERIC,
        wt: StandardSQLTypeName.BIGNUMERIC,
        qsec: StandardSQLTypeName.BIGNUMERIC,
        vs: StandardSQLTypeName.BIGNUMERIC,
        am: StandardSQLTypeName.INT64,
        gear: StandardSQLTypeName.INT64,
        carb: StandardSQLTypeName.INT64
    ]

    Reader reader = new InputStreamReader(this.getClass().getResourceAsStream('/mtcars.csv'))
    List<String[]> rowList = reader.readLines()*.split(',')
    List<Map> content = []
    def colNames = columns.keySet()
    rowList.each { row ->
      def rowMap = [:]
      row.eachWithIndex { String entry, int idx ->
        rowMap[colNames[idx]] = convert(entry, columns[idx])
      }
      content << rowMap
    }

    [
        columns: columns,
        content: content
    ]
  }
  @Test
  void testCreateAndInsert() {
    def data = createData()
    BigQueryWrapper bq = new BigQueryWrapper()
    if (bq.datasetExist(datasetName)) {
      bq.dropDataset(datasetName)
    }
    bq.createDataset(datasetName)
    (0..2).each {
      println("create insert drop round $it")
      Table table = bq.createTable(datasetName, tableName, data.columns)
      bq.insert(data.content, table)
      bq.dropTable(datasetName, tableName)
    }
  }

  private static Object convert(String val, StandardSQLTypeName standardSQLTypeName) {
    switch (standardSQLTypeName) {
      case StandardSQLTypeName.BIGNUMERIC -> new BigDecimal(val)
      case StandardSQLTypeName.INT64 -> Integer.valueOf(val)
      default -> val
    }
  }
}
