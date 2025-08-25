use sql_core::{Engine, Value, ValueType, parse_query};

#[test]
fn basic_flow() {
    let mut engine = Engine::new();
    engine.create_table(
        "users",
        vec![
            ("id".into(), ValueType::Int),
            ("name".into(), ValueType::Text),
        ],
    );

    let insert_q = parse_query("INSERT INTO users VALUES (1, 'Alice')").unwrap().1;
    engine.execute(insert_q).unwrap();

    let select_q = parse_query("SELECT * FROM users WHERE id=1").unwrap().1;
    let rows = engine.execute(select_q).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec![Value::Int(1), Value::Text("Alice".into())]);
}

#[test]
fn bool_flow() {
    let mut engine = Engine::new();
    engine.create_table(
        "flags",
        vec![
            ("id".into(), ValueType::Int),
            ("active".into(), ValueType::Bool),
        ],
    );

    let insert_q = parse_query("INSERT INTO flags VALUES (1, TRUE)").unwrap().1;
    engine.execute(insert_q).unwrap();

    let select_q = parse_query("SELECT * FROM flags WHERE active=TRUE").unwrap().1;
    let rows = engine.execute(select_q).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec![Value::Int(1), Value::Bool(true)]);
}

#[test]
fn advanced_select() {
    let mut engine = Engine::new();
    engine.create_table(
        "nums",
        vec![
            ("id".into(), ValueType::Int),
            ("name".into(), ValueType::Text),
        ],
    );

    let insert1 = parse_query("INSERT INTO nums VALUES (1, 'Alice')").unwrap().1;
    engine.execute(insert1).unwrap();
    let insert2 = parse_query("INSERT INTO nums VALUES (2, 'Bob')").unwrap().1;
    engine.execute(insert2).unwrap();
    let insert3 = parse_query("INSERT INTO nums VALUES (3, 'Carol')").unwrap().1;
    engine.execute(insert3).unwrap();

    let select_q = parse_query(
        "SELECT name FROM nums WHERE id>=1 ORDER BY id ASC LIMIT 1 OFFSET 1",
    )
    .unwrap()
    .1;
    let rows = engine.execute(select_q).unwrap();
    assert_eq!(rows, vec![vec![Value::Text("Bob".into())]]);
}
