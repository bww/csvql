pub mod frame;
pub mod error;

use frame::Frame;

#[derive(Debug, Clone)]
pub struct Query<F: Frame> {
  sources: Vec<Source<F>>,
  selectors: Vec<Selector>,
}

impl<F: Frame> Query<F> {
  pub fn new(sources: Vec<Source<F>>, selectors: Vec<Selector>) -> Query<F> {
    Query{
      sources: sources,
      selectors: selectors,
    }
  }
}

#[derive(Debug, Clone)]
pub struct Source<F: Frame> {
  name: String,
  data: F,
}

impl<F: Frame> Source<F> {
  pub fn new_with_data(name: &str, data: F) -> Source<F> {
    Source{
      name: name.to_owned(),
      data: data,
    }
  }
  
  pub fn name<'a>(&'a self) -> &'a str {
    &self.name
  }
  
  pub fn data<'a>(&'a self) -> &'a F {
    &self.data
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
  from: String,
  name: String,
  index: usize,
}

impl Column {
  pub fn new(from: &str, name: &str, index: usize) -> Column {
    Column{
      from: from.to_owned(),
      name: name.to_owned(),
      index: index,
    }
  }
  
  pub fn from<'a>(&'a self) &'a str {
    &self.from
  }
  
  pub fn name<'a>(&'a self) &'a str {
    &self.name
  }
  
  pub fn index<'a>(&'a self) usize {
    self.index
  }
}
