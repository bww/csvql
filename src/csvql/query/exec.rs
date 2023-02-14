use std::fmt;
use std::iter;
use std::collections::HashMap;

use nom;
use nom::bytes::complete::{tag, take_while_m_n};

use crate::csvql::query::frame;
use crate::csvql::query::frame::Frame;
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
  
  pub fn source(&mut self, name: &str) -> Option<&mut Box<dyn frame::Frame>> {
    self.sources.get_mut(name)
  }
  
  pub fn set_source(&mut self, name: &str, source: Box<dyn frame::Frame>) {
    self.sources.insert(name.to_owned(), source);
  }
}

#[derive(Clone)]
pub struct Join {
  on: Vec<(schema::QName, select::Order)>,
}

impl Join {
  pub fn new(on: Vec<(schema::QName, select::Order)>) -> Join {
    Join{
      on: on,
    }
  }
  
  pub fn on(on: (schema::QName, select::Order)) -> Join {
    Join{
      on: vec![on],
    }
  }
  
  pub fn columns<'a>(&'a self) -> &'a Vec<(schema::QName, select::Order)> {
    &self.on
  }
}

impl fmt::Display for Join {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "<{:?}>", &self.on)
  }
}

impl fmt::Debug for Join {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "<{:?}>", &self.on)
  }
}

pub struct Query {
  context: Context,
  from: String,
  join: Option<Join>,
  select: Vec<schema::QName>,
  schema: schema::Schema,
}

impl Query {
  pub fn new(context: Context, from: &str, join: Option<Join>, select: Vec<schema::QName>) -> Query {
    let schema = schema::Schema::new_with_keys(select.clone());
    Query{
      context: context,
      from: from.to_owned(),
      join: join,
      select: select,
      schema: schema,
    }
  }
}

impl frame::Frame for Query {
  fn name<'a>(&'a self) -> &'a str {
    "query"
  }
  
  fn schema<'a>(&'a self) -> &'a schema::Schema {
    &self.schema
  }
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, error::Error>> + 'a> {
    let mut frm: &mut Box<dyn frame::Frame> = match self.context.source(&self.from) {
      Some(frm) => frm,
      None => return Box::new(iter::once(Err(error::FrameError::new(&format!("No such frame: {}", &self.from)).into()))),
    };
    
    if let Some(join) = &self.join {
      for (on, sort) in join.columns() {
        let scope = match on.scope() {
          Some(scope) => scope,
          None => return Box::new(iter::once(Err(error::FrameError::new(&format!("Join defines no scope: {}", &on)).into()))),
        };
        let mut alt: &mut Box<dyn frame::Frame> = match self.context.source(scope) {
          Some(alt) => alt,
          None => return Box::new(iter::once(Err(error::FrameError::new(&format!("No such frame: {}", scope)).into()))),
        };
        let sorted = match frame::Sorted::new(alt, &select::Sort::on((on.clone(), sort.clone()))) {
          Ok(sorted) => sorted,
          Err(err) => return Box::new(iter::once(Err(err.into()))),
        };
        let joined = match frame::OuterJoin::new(frm, on, sorted, on) {
          Ok(joined) => joined,
          Err(err) => return Box::new(iter::once(Err(err.into()))),
        };
        frm = &mut Box::new(joined);
      }
    }
    
    // Box::new(iter::once(Err(error::FrameError::new("Unimplemented").into())))
    Box::new(frm.rows())
  }
}

impl fmt::Display for Query {
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
