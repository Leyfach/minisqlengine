use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::complete::{char, digit1, multispace0, multispace1},
    combinator::{map, map_res, opt},
    multi::{separated_list0, separated_list1},
    sequence::{delimited, preceded, separated_pair, tuple},
    IResult,
};

use crate::engine::Value;

#[derive(Debug, PartialEq)]
pub enum Operator {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, PartialEq)]
pub struct Condition {
    pub column: String,
    pub op: Operator,
    pub value: Value,
}

#[derive(Debug, PartialEq)]
pub struct SelectQuery {
    pub table: String,
    pub columns: Vec<String>,
    pub condition: Option<Condition>,
    pub order_by: Option<(String, bool)>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, PartialEq)]
pub struct InsertQuery {
    pub table: String,
    pub values: Vec<Value>,
}

#[derive(Debug, PartialEq)]
pub enum Query {
    Select(SelectQuery),
    Insert(InsertQuery),
}

fn identifier(i: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_')(i)
}

fn parse_operator(i: &str) -> IResult<&str, Operator> {
    alt((
        map(tag("<="), |_| Operator::Le),
        map(tag(">="), |_| Operator::Ge),
        map(tag("<>"), |_| Operator::Ne),
        map(tag("="), |_| Operator::Eq),
        map(tag("<"), |_| Operator::Lt),
        map(tag(">"), |_| Operator::Gt),
    ))(i)
}

fn parse_value(i: &str) -> IResult<&str, Value> {
    let parse_int = map_res(digit1, |s: &str| s.parse::<i64>().map(Value::Int));
    let parse_string = map(
        delimited(char('\''), take_while1(|c| c != '\''), char('\'')),
        |s: &str| Value::Text(s.to_string()),
    );
    let parse_bool = alt((
        map(tag_no_case("TRUE"), |_| Value::Bool(true)),
        map(tag_no_case("FALSE"), |_| Value::Bool(false)),
    ));
    alt((parse_int, parse_string, parse_bool))(i)
}

fn parse_values(i: &str) -> IResult<&str, Vec<Value>> {
    delimited(
        char('('),
        separated_list0(preceded(multispace0, char(',')), preceded(multispace0, parse_value)),
        char(')')
    )(i)
}

fn parse_condition(i: &str) -> IResult<&str, Condition> {
    map(
        tuple((
            identifier,
            preceded(multispace0, parse_operator),
            preceded(multispace0, parse_value),
        )),
        |(col, op, val)| Condition {
            column: col.to_string(),
            op,
            value: val,
        },
    )(i)
}

fn parse_columns(i: &str) -> IResult<&str, Vec<String>> {
    alt((
        map(tag("*"), |_| Vec::new()),
        map(
            separated_list1(preceded(multispace0, char(',')), preceded(multispace0, identifier)),
            |cols: Vec<&str>| cols.into_iter().map(|s| s.to_string()).collect(),
        ),
    ))(i)
}

fn parse_order_by(i: &str) -> IResult<&str, (String, bool)> {
    let (i, _) = tag("ORDER")(i)?;
    let (i, _) = multispace1(i)?;
    let (i, _) = tag("BY")(i)?;
    let (i, _) = multispace1(i)?;
    let (i, col) = identifier(i)?;
    let (i, dir) = opt(preceded(multispace1, alt((tag_no_case("ASC"), tag_no_case("DESC")))))(i)?;
    let asc = match dir {
        Some(d) => d.eq_ignore_ascii_case("ASC"),
        None => true,
    };
    Ok((i, (col.to_string(), asc)))
}

fn parse_usize(i: &str) -> IResult<&str, usize> {
    map_res(digit1, |s: &str| s.parse::<usize>())(i)
}

pub fn parse_select(i: &str) -> IResult<&str, SelectQuery> {
    let (i, _) = tag("SELECT")(i)?;
    let (i, _) = multispace0(i)?;
    let (i, columns) = parse_columns(i)?;
    let (i, _) = multispace0(i)?;
    let (i, _) = tag("FROM")(i)?;
    let (i, _) = multispace0(i)?;
    let (i, table) = identifier(i)?;
    let (i, _) = multispace0(i)?;
    let (i, condition) = opt(preceded(
        tag("WHERE"),
        preceded(multispace1, parse_condition),
    ))(i)?;
    let (i, _) = multispace0(i)?;
    let (i, order_by) = opt(parse_order_by)(i)?;
    let (i, _) = multispace0(i)?;
    let (i, limit) = opt(preceded(tag("LIMIT"), preceded(multispace1, parse_usize)))(i)?;
    let (i, _) = multispace0(i)?;
    let (i, offset) = opt(preceded(tag("OFFSET"), preceded(multispace1, parse_usize)))(i)?;
    Ok((
        i,
        SelectQuery {
            table: table.to_string(),
            columns,
            condition,
            order_by,
            limit,
            offset,
        },
    ))
}

pub fn parse_insert(i: &str) -> IResult<&str, InsertQuery> {
    let (i, _) = tag("INSERT")(i)?;
    let (i, _) = multispace1(i)?;
    let (i, _) = tag("INTO")(i)?;
    let (i, _) = multispace1(i)?;
    let (i, table) = identifier(i)?;
    let (i, _) = multispace1(i)?;
    let (i, _) = tag("VALUES")(i)?;
    let (i, _) = multispace0(i)?;
    let (i, values) = parse_values(i)?;
    Ok((
        i,
        InsertQuery {
            table: table.to_string(),
            values,
        },
    ))
}

pub fn parse_query(i: &str) -> IResult<&str, Query> {
    let (i, _) = multispace0(i)?;
    alt((
        map(parse_select, Query::Select),
        map(parse_insert, Query::Insert),
    ))(i)
}
