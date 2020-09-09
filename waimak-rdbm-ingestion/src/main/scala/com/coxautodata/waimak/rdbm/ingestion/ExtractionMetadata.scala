package com.coxautodata.waimak.rdbm.ingestion

trait ExtractionMetadata {
  def schemaName: String
  def tableName: String
  def primaryKeys: String
  def lastUpdatedColumn: Option[String]
  def qualifiedTableName(escapeKeyword: String => String): String
  def labelName: String = s"${schemaName}_$tableName"
  def transformTableName(transform: String => String): ExtractionMetadata

  // Fit with temporal metadata
  def historyTableSchema: Option[String] = None
  def historyTableName: Option[String] = None
  def startColName: Option[String] = None
  def endColName: Option[String] = None
  def databaseUpperTimestamp: Option[String] = None
  def setUpperTimestamp(ts: String): ExtractionMetadata

  def primaryKeysSeq: Seq[String] = pkCols

  def pkCols: Seq[String]
}

case class TableExtractionMetadata( schemaName: String
                                    , tableName: String
                                    , primaryKeys: String
                                    , lastUpdatedColumn: Option[String] = None) extends ExtractionMetadata {

  def qualifiedTableName(escapeKeyword: String => String): String =
    s"${escapeKeyword(schemaName)}.${escapeKeyword(tableName)}"

  override def transformTableName(transform: String => String): ExtractionMetadata = this.copy(tableName = transform(tableName))

  override def pkCols: Seq[String] =
    if (primaryKeys.contains(",")) primaryKeys.split(",").toSeq
    else Seq(primaryKeys)

  override def setUpperTimestamp(ts: String): ExtractionMetadata = this
}
object TableExtractionMetadata {
  /**
   * Create a new TableExtractionMetadata class, using the same arguments to the constructor other than for the primary
   * keys, which we map from a Seq[String] to a String.
   *
   * Note: Due to how the CaseClassConfigParser works this CANNOT be an apply method, otherwise the parser cannot work out
   * which apply to call and fails. Do not change this for apply.
   *
   * @param schemaName
   * @param tableName
   * @param primaryKeys
   * @param lastUpdatedColumn
   * @return
   */
  def fromPkSeq(schemaName: String, tableName: String, primaryKeys: Seq[String], lastUpdatedColumn: Option[String]): TableExtractionMetadata = {
    new TableExtractionMetadata(
      schemaName,
      tableName,
      primaryKeys.mkString(","),
      lastUpdatedColumn
    )
  }
}

case class SQLServerTemporalTableMetadata(schemaName: String
                                          , tableName: String
                                          , override val historyTableSchema: Option[String] = None
                                          , override val historyTableName: Option[String] = None
                                          , override val startColName: Option[String] = None
                                          , override val endColName: Option[String] = None
                                          , primaryKeys: String
                                          , override val databaseUpperTimestamp: Option[String] = Some("9999-12-31 23:59:59")) extends ExtractionMetadata {

  def mainTableMetadata: TableExtractionMetadata = TableExtractionMetadata.fromPkSeq(schemaName, tableName, pkCols, startColName)

  def isTemporal: Boolean = historyTableMetadata.isDefined

  def historyTableMetadata: Option[TableExtractionMetadata] = for {
    schema <- historyTableSchema
    table <- historyTableName
  } yield TableExtractionMetadata.fromPkSeq(schema, table, pkCols, endColName)

  override def lastUpdatedColumn: Option[String] = startColName

  override def qualifiedTableName(escapeKeyword: String => String): String = s"${escapeKeyword(schemaName)}.${escapeKeyword(tableName)}"

  override def transformTableName(transform: String => String): ExtractionMetadata = this.copy(tableName = transform(tableName))

  def setUpperTimestamp(ts: String): SQLServerTemporalTableMetadata = this.copy(databaseUpperTimestamp = Some(ts))

  override def pkCols: Seq[String] =
    if (primaryKeys.contains(";")) primaryKeys.split(";").toSeq
    else Seq(primaryKeys)
}
