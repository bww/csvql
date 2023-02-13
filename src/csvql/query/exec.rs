use std::collections::HashMap;

use nom;
use nom::bytes::complete::{tag, take_while_m_n};

use crate::csvql::query::frame;
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
  
  pub fn add_source(&mut self, name: &str, source: Box<dyn frame::Frame>) {
    self.sources.insert(name.to_owned(), source);
  }
}

pub struct Query<S: select::Selector, F: frame::Frame> {
  select: S,
  source: F,
}

// fn parse_query() -> Result<Query, error::Error> {
//   parse_select()
// }

// fn parse_select() -> Result<Query, error::Error> {
//   Err(error::ParseError::new(":(").into())
// }
