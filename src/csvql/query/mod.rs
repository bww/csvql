use std::io::Read;

use csv;

#[derive(Debug, Clone)]
pub struct Query<R: Read> {
  sources: Vec<Source<R>>,
  selectors: Vec<Selector>,
}

impl<R: Read> Query<R> {
  pub fn new(sources: Vec<Source<R>>, selectors: Vec<Selector>) -> Query<R> {
    Query{
      sources: sources,
      selectors: selectors,
    }
  }
}

#[derive(Debug, Clone)]
pub struct Source<R: Read> {
  name: String,
  data: Frame<R>,
}

impl<R: Read> Source<R> {
  pub fn new_with_data(name: &str, data: R) -> Source<R> {
    Source{
      name: name.to_owned(),
      data: Frame::new(data),
    }
  }
  
  pub fn name<'a>(&'a self) -> &'a str {
    &self.name
  }
}

pub trait Frame {
  fn rows();
}

#[derive(Debug, Clone)]
pub struct InputFrame<R: Read> {
  data: R,
}

impl<R: Read> InputFrame<R> {
  pub fn new(data: R) -> InputFrame<R> {
    InputFrame{
      data: data,
    }
  }
}

impl<R: Read> Frame for InputFrame<R> {
  fn rows(&mut self) {
    let _ = csv::Reader::from_reader(&mut self.data).records();
  }
}

#[derive(Debug, Clone)]
pub struct Selector {
  columns: Vec<Column>,
}

impl Selector {
  pub fn new_with_column(column: Column) -> Selector {
    Selector{
      columns: vec![column],
    }
  }
}

#[derive(Debug, Clone)]
pub struct Column {
  alias: String,
  name: String,
  index: u32,
}

impl Column {
  pub fn new(alias: &str, name: &str, index: u32) -> Column {
    Column{
      alias: alias.to_owned(),
      name: name.to_owned(),
      index: index,
    }
  }
}
