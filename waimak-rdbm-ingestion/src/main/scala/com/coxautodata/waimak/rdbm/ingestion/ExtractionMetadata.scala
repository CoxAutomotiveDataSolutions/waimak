package com.coxautodata.waimak.rdbm.ingestion

trait ExtractionMetadata {
  def schemaName: String
  def tableName: String
  def primaryKeys: String
  def pks: Seq[String]
  def lastUpdatedColumn: Option[String]
  def qualifiedTableName(escapeKeyword: String => String): String
  def labelName: String = s"${schemaName}_$tableName"
  def transformTableName(transform: String => String): ExtractionMetadata

  // Fit with temporal metadata
  def historyTableSchema: Option[String] = None
  def historyTableName: Option[String] = None
  def startColName: Option[String] = None
  def endColName: Option[String] = None
}

case class TableExtractionMetadata( schemaName: String
                                    , tableName: String
                                    , pks: Seq[String]
                                    , lastUpdatedColumn: Option[String] = None) extends ExtractionMetadata {
  def qualifiedTableName(escapeKeyword: String => String): String =
    s"${escapeKeyword(schemaName)}.${escapeKeyword(tableName)}"

  override def transformTableName(transform: String => String): ExtractionMetadata = this.copy(tableName = transform(tableName))

  override def primaryKeys: String = pks.mkString(";")
}

case class SQLServerTemporalTableMetadata(schemaName: String
                                          , tableName: String
                                          , override val historyTableSchema: Option[String] = None
                                          , override val historyTableName: Option[String] = None
                                          , override val startColName: Option[String] = None
                                          , override val endColName: Option[String] = None
                                          , primaryKeys: String) extends ExtractionMetadata {

  def pkCols: Seq[String] =
    if (primaryKeys.contains(";")) primaryKeys.split(";").toSeq
    else Seq(primaryKeys)

  def mainTableMetadata: TableExtractionMetadata = TableExtractionMetadata(schemaName, tableName, pkCols, startColName)

  def isTemporal: Boolean = historyTableMetadata.isDefined

  def historyTableMetadata: Option[TableExtractionMetadata] = for {
    schema <- historyTableSchema
    table <- historyTableName
  } yield TableExtractionMetadata(schema, table, pkCols, endColName)

  override def lastUpdatedColumn: Option[String] = startColName

  override def qualifiedTableName(escapeKeyword: String => String): String = s"${escapeKeyword(schemaName)}.${escapeKeyword(tableName)}"

  override def transformTableName(transform: String => String): ExtractionMetadata = this.copy(tableName = transform(tableName))

  override def pks: Seq[String] = pkCols
}
