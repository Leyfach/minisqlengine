use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Value {
    Int(i64),
    Text(String),
    Bool(bool),
    Null,
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use Value::*;
        match (self, other) {
            (Int(a), Int(b)) => a.partial_cmp(b),
            (Text(a), Text(b)) => a.partial_cmp(b),
            (Bool(a), Bool(b)) => a.partial_cmp(b),
            (Null, Null) => Some(Ordering::Equal),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum ColumnType {
    Int,
    Text,
    Bool,
    Null,
}

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub typ: ColumnType,
    pub indexed: bool,
}

#[derive(Debug)]
struct Table {
    columns: Vec<Column>,
    rows: Vec<Vec<Value>>,
    indexes: HashMap<String, HashMap<Value, Vec<usize>>>,
}

#[derive(Debug)]
pub struct Engine {
    tables: HashMap<String, Table>,
}

#[derive(Debug, PartialEq)]
pub enum Error {
    TableNotFound,
    ColumnNotFound,
    TypeMismatch,
}

impl Engine {
    pub fn new() -> Self {
        Self { tables: HashMap::new() }
    }

    pub fn create_table(&mut self, name: &str, columns: Vec<Column>) {
        self.tables.insert(
            name.to_string(),
            Table { columns, rows: Vec::new(), indexes: HashMap::new() },
        );
    }

    pub fn insert(&mut self, table: &str, row: Vec<Value>) -> Result<(), Error> {
        let t = self.tables.get_mut(table).ok_or(Error::TableNotFound)?;
        if row.len() != t.columns.len() {
            return Err(Error::TypeMismatch);
        }
        for (val, col) in row.iter().zip(t.columns.iter()) {
            if !matches_type(val, &col.typ) {
                return Err(Error::TypeMismatch);
            }
        }
        let idx = t.rows.len();
        t.rows.push(row.clone());
        for (i, col) in t.columns.iter().enumerate() {
            if col.indexed {
                let index = t.indexes.entry(col.name.clone()).or_insert_with(HashMap::new);
                index.entry(row[i].clone()).or_default().push(idx);
            }
        }
        Ok(())
    }

    pub fn select(&self, table: &str, query: Query) -> Result<Vec<Vec<Value>>, Error> {
        let t = self.tables.get(table).ok_or(Error::TableNotFound)?;
        let col_map: HashMap<&str, usize> =
            t.columns.iter().enumerate().map(|(i, c)| (c.name.as_str(), i)).collect();

        if let Some(ref cols) = query.columns {
            for c in cols {
                if !col_map.contains_key(c.as_str()) {
                    return Err(Error::ColumnNotFound);
                }
            }
        }

        let mut indices: Vec<usize> = (0..t.rows.len()).collect();

        if let Some(expr) = query.filter {
            if let Some((col, val)) = expr.as_index_lookup(&col_map) {
                if let Some(index) = t.indexes.get(col) {
                    if let Some(rows) = index.get(&val) {
                        indices = rows.clone();
                    } else {
                        indices = Vec::new();
                    }
                } else {
                    indices.retain(|&i| expr.eval_bool(&t.rows[i], &col_map));
                }
            } else {
                indices.retain(|&i| expr.eval_bool(&t.rows[i], &col_map));
            }
        }

        if let Some((col, asc)) = &query.order_by {
            let idx = *col_map.get(col.as_str()).ok_or(Error::ColumnNotFound)?;
            indices.sort_by(|&a, &b| {
                t.rows[a][idx]
                    .partial_cmp(&t.rows[b][idx])
                    .unwrap_or(Ordering::Equal)
            });
            if !asc {
                indices.reverse();
            }
        }

        let offset = query.offset.unwrap_or(0);
        let indices = indices.into_iter().skip(offset).collect::<Vec<_>>();
        let mut rows: Vec<Vec<Value>> = indices
            .iter()
            .map(|&i| match &query.columns {
                Some(cols) => cols
                    .iter()
                    .map(|c| {
                        let idx = col_map[c.as_str()];
                        t.rows[i][idx].clone()
                    })
                    .collect(),
                None => t.rows[i].clone(),
            })
            .collect();

        if let Some(limit) = query.limit {
            rows.truncate(limit);
        }

        Ok(rows)
    }
}

fn matches_type(v: &Value, t: &ColumnType) -> bool {
    match (v, t) {
        (Value::Int(_), ColumnType::Int) => true,
        (Value::Text(_), ColumnType::Text) => true,
        (Value::Bool(_), ColumnType::Bool) => true,
        (Value::Null, _) => true,
        _ => false,
    }
}

#[derive(Clone)]
pub enum Expr {
    Eq(Box<Expr>, Box<Expr>),
    Neq(Box<Expr>, Box<Expr>),
    Lt(Box<Expr>, Box<Expr>),
    Lte(Box<Expr>, Box<Expr>),
    Gt(Box<Expr>, Box<Expr>),
    Gte(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Column(String),
    Value(Value),
}
impl Expr {
    fn eval(&self, row: &Vec<Value>, cols: &HashMap<&str, usize>) -> Value {
        match self {
            Expr::Column(c) => row[*cols.get(c.as_str()).expect("col")].clone(),
            Expr::Value(v) => v.clone(),
            Expr::Eq(a, b) => Value::Bool(a.eval(row, cols) == b.eval(row, cols)),
            Expr::Neq(a, b) => Value::Bool(a.eval(row, cols) != b.eval(row, cols)),
            Expr::Lt(a, b) => Value::Bool(a.eval(row, cols) < b.eval(row, cols)),
            Expr::Lte(a, b) => Value::Bool(a.eval(row, cols) <= b.eval(row, cols)),
            Expr::Gt(a, b) => Value::Bool(a.eval(row, cols) > b.eval(row, cols)),
            Expr::Gte(a, b) => Value::Bool(a.eval(row, cols) >= b.eval(row, cols)),
            Expr::And(a, b) => {
                if let (Value::Bool(ba), Value::Bool(bb)) = (a.eval(row, cols), b.eval(row, cols)) {
                    Value::Bool(ba && bb)
                } else {
                    Value::Bool(false)
                }
            }
            Expr::Or(a, b) => {
                if let (Value::Bool(ba), Value::Bool(bb)) = (a.eval(row, cols), b.eval(row, cols)) {
                    Value::Bool(ba || bb)
                } else {
                    Value::Bool(false)
                }
            }
        }
    }

    fn eval_bool(&self, row: &Vec<Value>, cols: &HashMap<&str, usize>) -> bool {
        match self.eval(row, cols) {
            Value::Bool(b) => b,
            _ => false,
        }
    }

    fn as_index_lookup<'a>(&'a self, _cols: &HashMap<&str, usize>) -> Option<(&'a str, Value)> {
        match self {
            Expr::Eq(a, b) => match (&**a, &**b) {
                (Expr::Column(c), Expr::Value(v)) => Some((c.as_str(), v.clone())),
                (Expr::Value(v), Expr::Column(c)) => Some((c.as_str(), v.clone())),
                _ => None,
            },
            _ => None,
        }
    }
}

pub struct Query {
    pub columns: Option<Vec<String>>,
    pub filter: Option<Expr>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub order_by: Option<(String, bool)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_engine() -> Engine {
        let mut eng = Engine::new();
        eng.create_table(
            "users",
            vec![
                Column { name: "id".into(), typ: ColumnType::Int, indexed: true },
                Column { name: "name".into(), typ: ColumnType::Text, indexed: false },
                Column { name: "active".into(), typ: ColumnType::Bool, indexed: false },
            ],
        );
        eng
    }

    #[test]
    fn insert_type_check() {
        let mut eng = sample_engine();
        let res = eng.insert(
            "users",
            vec![Value::Int(1), Value::Text("Alice".into()), Value::Bool(true)],
        );
        assert!(res.is_ok());

        let res = eng.insert(
            "users",
            vec![Value::Text("wrong".into()), Value::Text("Bob".into()), Value::Bool(false)],
        );
        assert_eq!(res, Err(Error::TypeMismatch));
    }

    #[test]
    fn select_with_filter_and_order() {
        let mut eng = sample_engine();
        eng.insert("users", vec![Value::Int(1), Value::Text("Alice".into()), Value::Bool(true)]).unwrap();
        eng.insert("users", vec![Value::Int(2), Value::Text("Bob".into()), Value::Bool(false)]).unwrap();
        eng.insert("users", vec![Value::Int(3), Value::Text("Carol".into()), Value::Bool(true)]).unwrap();

        let query = Query {
            columns: Some(vec!["name".into()]),
            filter: Some(Expr::And(
                Box::new(Expr::Gt(Box::new(Expr::Column("id".into())), Box::new(Expr::Value(Value::Int(1))))),
                Box::new(Expr::Eq(Box::new(Expr::Column("active".into())), Box::new(Expr::Value(Value::Bool(true))))),
            )),
            limit: Some(1),
            offset: Some(0),
            order_by: Some(("id".into(), false)),
        };
        let rows = eng.select("users", query).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Text("Carol".into()));
    }

    #[test]
    fn index_lookup() {
        let mut eng = sample_engine();
        eng.insert("users", vec![Value::Int(1), Value::Text("Alice".into()), Value::Bool(true)]).unwrap();
        eng.insert("users", vec![Value::Int(2), Value::Text("Bob".into()), Value::Bool(false)]).unwrap();

        let query = Query {
            columns: None,
            filter: Some(Expr::Eq(
                Box::new(Expr::Column("id".into())),
                Box::new(Expr::Value(Value::Int(2))),
            )),
            limit: None,
            offset: None,
            order_by: None,
        };
        let rows = eng.select("users", query).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::Text("Bob".into()));
    }
}

