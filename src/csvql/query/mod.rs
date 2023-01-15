use std::io::Read;

use csv;

struct Query<R: Read> {
  sources: Vec<Source<R>>,
  select: Vec<Selector<R>>,
}

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

struct Source<R: Read> {
  name: String,
  data: Frame<R>,
}

struct Selector<R: Read> {
  columns: Vec<Column<R>>,
}

struct Column<R: Read> {
  source: Source<R>,
  column: String,
  index: u32,
}
