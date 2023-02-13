use std::fmt;
use std::iter;
use std::collections::HashMap;

use nom;
use nom::bytes::complete::{tag, take_while_m_n};

use crate::csvql::query::frame;
use crate::csvql::query::schema;
use crate::csvql::query::select;
use crate::csvql::query::error;

pub struct Context {
  sources: HashMap<String, Box<dyn frame::Frame>>,
}

impl Context {
  pub fn new() -> Context {
    Context{
      sources: HashMap::new(),
    }
  }
  
  pub fn source(&self, name: &str) -> Option<&Box<dyn frame::Frame>> {
    self.sources.get(name)
  }
  
  pub fn set_source(&mut self, name: &str, source: Box<dyn frame::Frame>) {
    self.sources.insert(name.to_owned(), source);
  }
}

pub struct Query<'a, S: select::Selector> {
  context: &'a Context,
  select: S,
}

impl<'a, S: select::Selector> Query<'a, S> {
  pub fn new(context: &'a Context, select: S) -> Query<'a, S> {
    Query{
      context: context,
      select: select,
    }
  }
}

impl<'a, S: select::Selector> frame::Frame for Query<'a, S> {
  fn name<'b>(&'b self) -> &'b str {
    "query"
  }
  
  fn schema<'b>(&'b self) -> &'b schema::Schema {
    self.select.schema()
  }
  
  fn rows<'b>(&'b mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, error::Error>> + 'b> {
    Box::new(iter::once(Err(error::FrameError::new("Unimplemented").into())))
  }
}

impl<'a, S: select::Selector> fmt::Display for Query<'a, S> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.name())
  }
}

// fn parse_query() -> Result<Query, error::Error> {
//   parse_select()
// }

// fn parse_select() -> Result<Query, error::Error> {
//   Err(error::ParseError::new(":(").into())
// }
