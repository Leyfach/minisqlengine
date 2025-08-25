use sql_core::{Engine, Value};

#[test]
fn basic_flow() {
    let mut engine = Engine::new();
    engine.create_table("users", vec!["id".into(), "name".into()]);
    engine.insert_into(
        "users",
        vec![Value::Int(1), Value::Text("Alice".into())],
    );

    let rows = engine.select_all_where("users", "id", &Value::Int(1));
    assert_eq!(rows.len(), 1);
}
