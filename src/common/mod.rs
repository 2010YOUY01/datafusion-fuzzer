use datafusion::arrow::datatypes::SchemaRef;

#[derive(Debug, Clone)]
pub struct LogicalTable {
    pub name: String,
    pub schema: SchemaRef,
    pub table_type: LogicalTableType,
}

#[derive(Debug, Clone)]
pub enum LogicalTableType {
    Table,
    View,
    Subquery(String),
}

impl LogicalTable {
    pub fn new(name: String, schema: SchemaRef, table_type: LogicalTableType) -> Self {
        Self {
            name,
            schema,
            table_type,
        }
    }
}
