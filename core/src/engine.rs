use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use crate::parser::Query;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Int(i64),
    Text(String),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ValueType {
    Int,
    Text,
    Bool,
    Null,
}

impl Value {
    pub fn value_type(&self) -> ValueType {
        match self {
            Value::Int(_) => ValueType::Int,
            Value::Text(_) => ValueType::Text,
            Value::Bool(_) => ValueType::Bool,
            Value::Null => ValueType::Null,
        }
    }
}

pub type Row = Vec<Value>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EngineError {
    TableNotFound(String),
    ColumnNotFound(String),
    ValueCountMismatch,
    TypeMismatch {
        column: String,
        expected: ValueType,
        found: ValueType,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub col_type: ValueType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
}

impl Table {
    pub fn new(columns: Vec<(String, ValueType)>) -> Self {
        let cols = columns
            .into_iter()
            .map(|(name, col_type)| Column { name, col_type })
            .collect();
        Self {
            columns: cols,
            rows: Vec::new(),
        }
    }

    pub fn insert(&mut self, values: Row) {
        self.rows.push(values);
    }
}

#[derive(Default)]
pub struct Engine {
    pub tables: HashMap<String, Table>,
}

impl Engine {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, name: &str, columns: Vec<(String, ValueType)>) {
        self.tables.insert(name.to_string(), Table::new(columns));
    }

    pub fn insert_into(&mut self, name: &str, values: Row) -> Result<(), EngineError> {
        match self.tables.get_mut(name) {
            Some(table) => {
                if table.columns.len() != values.len() {
                    return Err(EngineError::ValueCountMismatch);
                }
                for (col, val) in table.columns.iter().zip(values.iter()) {
                    if col.col_type != val.value_type() {
                        return Err(EngineError::TypeMismatch {
                            column: col.name.clone(),
                            expected: col.col_type.clone(),
                            found: val.value_type(),
                        });
                    }
                }
                table.insert(values);
                Ok(())
            }
            None => Err(EngineError::TableNotFound(name.to_string())),
        }
    }

    pub fn select_all_where(
        &self,
        name: &str,
        column: &str,
        value: &Value,
    ) -> Result<Vec<Row>, EngineError> {
        let table = self
            .tables
            .get(name)
            .ok_or_else(|| EngineError::TableNotFound(name.to_string()))?;
        let idx = table
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| EngineError::ColumnNotFound(column.to_string()))?;
        Ok(
            table
                .rows
                .iter()
                .cloned()
                .filter(|r| r.get(idx) == Some(value))
                .collect(),
        )
    }

    pub fn execute(&mut self, query: crate::parser::Query) -> Result<Vec<Row>, EngineError> {
        match query {
            crate::parser::Query::Select(q) => self.select_all_where(&q.table, &q.column, &q.value),
            crate::parser::Query::Insert(q) => {
                self.insert_into(&q.table, q.values)?;
                Ok(Vec::new())
            }
        }
    }
}
