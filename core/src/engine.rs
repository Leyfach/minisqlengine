use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Int(i64),
    Text(String),
    Null,
}

pub type Row = Vec<Value>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
}

impl Table {
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            columns,
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

    pub fn create_table(&mut self, name: &str, columns: Vec<String>) {
        self.tables.insert(name.to_string(), Table::new(columns));
    }

    pub fn insert_into(&mut self, name: &str, values: Row) {
        if let Some(table) = self.tables.get_mut(name) {
            table.insert(values);
        }
    }

    pub fn select_all_where(&self, name: &str, column: &str, value: &Value) -> Vec<Row> {
        if let Some(table) = self.tables.get(name) {
            if let Some(idx) = table.columns.iter().position(|c| c == column) {
                return table
                    .rows
                    .iter()
                    .cloned()
                    .filter(|r| r.get(idx) == Some(value))
                    .collect();
            }
        }
        Vec::new()
    }
}
