package se.alipsa.bq

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.Dataset
import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bigquery.DatasetInfo
import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.InsertAllResponse
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.Table
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.TableInfo

class BigQueryWrapper {

  private BigQuery bigQuery
  private String projectId

  BigQueryWrapper() {
    String projectId = System.getenv('GOOGLE_CLOUD_PROJECT')
    if (projectId == null) {
      throw new RuntimeException("Please set the environment variable GOOGLE_CLOUD_PROJECT prior to creating this class (or pass it as a parameter)")
    }
    this.projectId = projectId
    bigQuery = BigQueryOptions.newBuilder()
        .setProjectId(projectId)
        .build()
        .getService()
  }

  Dataset createDataset(String datasetName, String description = null) throws Exception {
    try {
      Dataset dataset = bigQuery.getDataset(DatasetId.of(datasetName))
      if (dataset != null) {
        System.out.println("Dataset $datasetName already exists.")
        return dataset
      }
      def builder = DatasetInfo.newBuilder(datasetName)
      if (description != null) {
        builder.setDescription(description)
      }
      DatasetInfo datasetInfo = builder.build()
      Dataset ds = bigQuery.create(datasetInfo)
      String newDatasetName = ds.getDatasetId().getDataset()
      System.out.println(newDatasetName + " created successfully")
      ds
    } catch (BigQueryException e) {
      throw new Exception("Dataset was not created: " + e.toString(), e)
    }
  }

  private static Schema createSchema(Map columns) {
    def fields = []
    columns.each {
      fields << Field.of(it.key, it.value)
    }
    Schema.of(fields as Field[])
  }

  Table createTable(String datasetName, String tableName, Map columnDefinitions) {
    println "Createing table $tableName in $datasetName"
    Schema schema = createSchema(columnDefinitions)
    TableId tableId = TableId.of(projectId, datasetName, tableName)
    TableDefinition tableDefinition = StandardTableDefinition.of(schema)
    TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build()
    bigQuery.create(tableInfo)
  }

  boolean dropTable(String datasetName, String tableName) throws Exception {
    try {
      boolean success = bigQuery.delete(TableId.of(datasetName, tableName))
      if (success) {
        println("Table ${datasetName}.${tableName} deleted successfully")
      } else {
        println("Table ${datasetName}.${tableName} was not found")
      }
      return success
    } catch (BigQueryException e) {
      throw new Exception(e)
    }
  }

  InsertAllResponse insert(List<Map> rowlist, Table table) {
    System.out.println("Inserting ${rowlist.size()} rows into ${table.tableId.table}".toString())
    try {
      def builder = InsertAllRequest.newBuilder(table.tableId)
      rowlist.each {
        builder.addRow(it)
      }
      bigQuery.insertAll(builder.build())
    } catch (BigQueryException e) {
      throw new Exception(e)
    }
  }

  boolean datasetExist(String datasetName) throws Exception {
    try {
      Dataset dataset = bigQuery.getDataset(DatasetId.of(datasetName));
      if (dataset != null) {
        return true
      }
      return false
    } catch (BigQueryException e) {
      throw new Exception(e)
    }
  }

  boolean dropDataset(String datasetName) throws Exception {
    try {
      DatasetId datasetId = DatasetId.of(projectId, datasetName)
      boolean success = bigQuery.delete(datasetId, BigQuery.DatasetDeleteOption.deleteContents())
      if (success) {
        System.out.println("Dataset $datasetName deleted successfully");
      } else {
        System.out.println("Dataset $datasetName was not found");
      }
      return success
    } catch (BigQueryException e) {
      throw new Exception(e)
    }
  }
}
