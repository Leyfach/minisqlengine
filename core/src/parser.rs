use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::complete::{char, digit1, multispace0, multispace1},
    combinator::{map, map_res},
    multi::separated_list0,
    sequence::{delimited, preceded, separated_pair},
    IResult,
};

use crate::engine::Value;

#[derive(Debug, PartialEq)]
pub struct SelectQuery {
    pub table: String,
    pub column: String,
    pub value: Value,
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

pub fn parse_select(i: &str) -> IResult<&str, SelectQuery> {
    let (i, _) = tag("SELECT")(i)?;
    let (i, _) = multispace0(i)?;
    let (i, _) = tag("*")(i)?;
    let (i, _) = multispace0(i)?;
    let (i, _) = tag("FROM")(i)?;
    let (i, _) = multispace0(i)?;
    let (i, table) = identifier(i)?;
    let (i, _) = multispace0(i)?;
    let (i, _) = tag("WHERE")(i)?;
    let (i, _) = multispace0(i)?;
    let (i, (column, value)) = separated_pair(
        identifier,
        preceded(multispace0, char('=')),
        parse_value,
    )(i)?;
    Ok((
        i,
        SelectQuery {
            table: table.to_string(),
            column: column.to_string(),
            value,
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
