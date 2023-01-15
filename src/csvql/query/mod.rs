use std::io::Read;

use csv;

#[derive(Debug, Clone)]
struct Query<R: Read> {
  sources: Vec<Source<R>>,
  selectors: Vec<Selector<R>>,
}

impl<R: Read> Query<R> {
  fn new(sources: Vec<Source<R>>, selectors: Vec<Selector<R>>) -> Query<R> {
    Query{
      sources: sources,
      selectors: selectors,
    }
  }
}

#[derive(Debug, Clone)]
struct Source<R: Read> {
  name: String,
  data: Frame<R>,
}

#[derive(Debug, Clone)]
struct Frame<R: Read> {
  data: R,
}

impl<R: Read> Frame<R> {
  fn new<'a>(data: &'a mut R) -> Frame<&'a mut R> {
    Frame{
      data: data,
    }
  }
  
  fn rows(&mut self) {
    let _ = csv::Reader::from_reader(&mut self.data).records();
  }
}

#[derive(Debug, Clone)]
struct Selector<R: Read> {
  columns: Vec<Column<R>>,
}

#[derive(Debug, Clone)]
struct Column<R: Read> {
  source: Source<R>,
  column: String,
  index: u32,
}
