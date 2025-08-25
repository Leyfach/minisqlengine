use std::collections::HashMap;

use crate::parser::{Condition, Operator, Query, SelectQuery};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Value {
    Int(i64),
    Text(String),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    pub indices: HashMap<String, HashMap<Value, Vec<usize>>>,
}

impl Table {
    pub fn new(columns: Vec<(String, ValueType)>) -> Self {
        let cols = columns
            .into_iter()
            .map(|(name, col_type)| Column { name, col_type })
            .collect::<Vec<_>>();
        Self {
            columns: cols,
            rows: Vec::new(),
            indices: HashMap::new(),
        }
    }

    pub fn create_index(&mut self, column: &str) {
        if let Some(pos) = self.columns.iter().position(|c| c.name == column) {
            let mut map: HashMap<Value, Vec<usize>> = HashMap::new();
            for (idx, row) in self.rows.iter().enumerate() {
                if let Some(val) = row.get(pos) {
                    map.entry(val.clone()).or_default().push(idx);
                }
            }
            self.indices.insert(column.to_string(), map);
        }
    }

    pub fn insert(&mut self, values: Row) {
        let row_idx = self.rows.len();
        for (col_idx, value) in values.iter().enumerate() {
            if let Some(col) = self.columns.get(col_idx) {
                if let Some(index) = self.indices.get_mut(&col.name) {
                    index.entry(value.clone()).or_default().push(row_idx);
                }
            }
        }
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
        let mut table = Table::new(columns);
        if let Some(first_col) = table.columns.get(0) {
            table.create_index(&first_col.name);
        }
        self.tables.insert(name.to_string(), table);
    }

    pub fn insert_into(
        &mut self,
        name: &str,
        values: Row,
        columns: Option<Vec<String>>,
    ) -> Result<(), EngineError> {
        match self.tables.get_mut(name) {
            Some(table) => {
                if let Some(cols) = columns {
                    if cols.len() != values.len() {
                        return Err(EngineError::ValueCountMismatch);
                    }
                    let mut row = vec![Value::Null; table.columns.len()];
                    for (col_name, val) in cols.iter().zip(values.iter()) {
                        let idx = table
                            .columns
                            .iter()
                            .position(|c| c.name == *col_name)
                            .ok_or_else(|| EngineError::ColumnNotFound(col_name.clone()))?;
                        let col_def = &table.columns[idx];
                        if col_def.col_type != val.value_type() {
                            return Err(EngineError::TypeMismatch {
                                column: col_def.name.clone(),
                                expected: col_def.col_type.clone(),
                                found: val.value_type(),
                            });
                        }
                        row[idx] = val.clone();
                    }
                    table.insert(row);
                    Ok(())
                } else {
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
            }
            None => Err(EngineError::TableNotFound(name.to_string())),
        }
    }

    fn get_column_idx(table: &Table, name: &str) -> Result<usize, EngineError> {
        table
            .columns
            .iter()
            .position(|c| c.name == name)
            .ok_or_else(|| EngineError::ColumnNotFound(name.to_string()))
    }

    fn compare(a: &Value, op: &Operator, b: &Value) -> bool {
        match (a, b) {
            (Value::Int(x), Value::Int(y)) => match op {
                Operator::Eq => x == y,
                Operator::Ne => x != y,
                Operator::Lt => x < y,
                Operator::Le => x <= y,
                Operator::Gt => x > y,
                Operator::Ge => x >= y,
            },
            (Value::Text(x), Value::Text(y)) => match op {
                Operator::Eq => x == y,
                Operator::Ne => x != y,
                Operator::Lt => x < y,
                Operator::Le => x <= y,
                Operator::Gt => x > y,
                Operator::Ge => x >= y,
            },
            (Value::Bool(x), Value::Bool(y)) => match op {
                Operator::Eq => x == y,
                Operator::Ne => x != y,
                _ => false,
            },
            _ => false,
        }
    }

    pub fn select(&self, q: &SelectQuery) -> Result<Vec<Row>, EngineError> {
        let table = self
            .tables
            .get(&q.table)
            .ok_or_else(|| EngineError::TableNotFound(q.table.clone()))?;

        let mut rows: Vec<Row> = if let Some(cond) = &q.condition {
            let col_idx = Self::get_column_idx(table, &cond.column)?;
            if let Operator::Eq = cond.op {
                if let Some(index) = table.indices.get(&cond.column) {
                    if let Some(row_indices) = index.get(&cond.value) {
                        row_indices.iter().map(|&i| table.rows[i].clone()).collect()
                    } else {
                        Vec::new()
                    }
                } else {
                    table
                        .rows
                        .iter()
                        .cloned()
                        .filter(|r| Self::compare(&r[col_idx], &cond.op, &cond.value))
                        .collect()
                }
            } else {
                table
                    .rows
                    .iter()
                    .cloned()
                    .filter(|r| Self::compare(&r[col_idx], &cond.op, &cond.value))
                    .collect()
            }
        } else {
            table.rows.clone()
        };

        if let Some((ref col, asc)) = q.order_by {
            let idx = Self::get_column_idx(table, col)?;
            rows.sort_by(|a, b| {
                let va = &a[idx];
                let vb = &b[idx];
                match (va, vb) {
                    (Value::Int(x), Value::Int(y)) => x.cmp(y),
                    (Value::Text(x), Value::Text(y)) => x.cmp(y),
                    (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
                    _ => std::cmp::Ordering::Equal,
                }
            });
            if !asc {
                rows.reverse();
            }
        }

        let start = q.offset.unwrap_or(0);
        let mut rows = if start >= rows.len() {
            Vec::new()
        } else {
            rows.into_iter().skip(start).collect::<Vec<_>>()
        };
        if let Some(limit) = q.limit {
            if rows.len() > limit {
                rows.truncate(limit);
            }
        }

        let result = if q.columns.is_empty() {
            rows
        } else {
            let indices: Result<Vec<usize>, EngineError> = q
                .columns
                .iter()
                .map(|c| Self::get_column_idx(table, c))
                .collect();
            let indices = indices?;
            rows.into_iter()
                .map(|r| indices.iter().map(|&i| r[i].clone()).collect())
                .collect()
        };
        Ok(result)
    }

    pub fn execute(&mut self, query: crate::parser::Query) -> Result<Vec<Row>, EngineError> {
        match query {
            crate::parser::Query::Select(q) => self.select(&q),
            crate::parser::Query::Insert(q) => {
                self.insert_into(&q.table, q.values, q.columns)?;
                Ok(Vec::new())
            }
        }
    }
}
