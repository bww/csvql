use std::io;
use std::fmt;
use std::iter;
use std::collections::HashSet;
use std::collections::HashMap;
use std::collections::BTreeMap;

use csv;

use crate::csvql::query::error;

fn convert_record(e: csv::Result<csv::StringRecord>) -> Result<csv::StringRecord, error::Error> {
  match e {
    Ok(v)    => Ok(v),
    Err(err) => Err(err.into()),
  }
}

#[derive(Clone)]
pub struct Schema {
  cmap: HashMap<String, usize>,
  keys: Vec<String>,
}

impl Schema {
  pub fn empty() -> Schema {
    Schema{
      cmap: HashMap::new(),
      keys: Vec::new(),
    }
  }
  
  pub fn new_from_headers(hdrs: &csv::StringRecord) -> Schema {
    let mut keys: Vec<String> = Vec::new();
    for hdr in hdrs {
      keys.push(hdr.to_string());
    }
    
    keys.sort();
    
    let mut cmap: HashMap<String, usize> = HashMap::new();
    for (i, k) in keys.iter().enumerate() {
      cmap.insert(k.to_string(), i);
    }
    
    Schema{
      cmap: cmap,
      keys: keys,
    }
  }
  
  pub fn union(&self, with: &Schema) -> Schema {
    let mut cols: HashSet<String> = HashSet::new();
    for (k, _) in &self.cmap {
      cols.insert(k.to_owned());
    }
    for (k, _) in &with.cmap {
      cols.insert(k.to_owned());
    }
    
    let mut keys: Vec<String> = Vec::new();
    for k in cols.into_iter() {
      keys.push(k);
    }
    
    keys.sort();
    
    let mut cmap: HashMap<String, usize> = HashMap::new();
    for (i, k) in keys.iter().enumerate() {
      cmap.insert(k.to_owned(), i);
    }
    
    Schema{
      cmap: cmap,
      keys: keys,
    }
  }
  
  pub fn columns<'a>(&'a self) -> Vec<&'a String> {
    self.keys.iter().collect()
  }
  
  pub fn description(&self, debug: bool) -> String {
    let mut dsc = String::new();
    let mut n = 0;
    for key in &self.keys {
      if n > 0 {
        dsc.push_str(", ");
      }
      if debug {
        dsc.push_str(&format!("{}={}", &key, n));
      }else{
        dsc.push_str(&key);
      }
      n += 1;
    }
    dsc
  }
  
  pub fn index(&self, name: &str) -> Option<usize> {
    match self.cmap.get(name) {
      Some(index) => Some(*index),
      None => None,
    }
  }
}

impl fmt::Display for Schema {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.description(false))
  }
}

impl fmt::Debug for Schema {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.description(true))
  }
}

// A frame of data
pub trait Frame {
  fn name<'a>(&'a self) -> &'a str;
  fn schema<'a>(&'a self) -> &'a Schema;
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, error::Error>> + 'a>;
}

impl<F: Frame + ?Sized> Frame for Box<F> { // black magic
  fn name<'a>(&'a self) -> &'a str {
    (**self).name()
  }
  
  fn schema<'a>(&'a self) -> &'a Schema {
    (**self).schema()
  }
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, error::Error>> + 'a> {
    (**self).rows()
  }
}

// A random-access frame indexed on a particular column
pub trait Index: Frame {
  fn on<'a>(&'a self) -> &'a str; // the indexed column
  fn get<'a>(&'a self, key: &str) -> Result<&'a csv::StringRecord, error::Error>;
}

impl<I: Index + ?Sized> Index for Box<I> { // black magic
  fn on<'a>(&'a self) -> &'a str {
    (**self).on()
  }
  
  fn get<'a>(&'a self, key: &str) -> Result<&'a csv::StringRecord, error::Error> {
    (**self).get(key)
  }
}

// A btree indexed frame
#[derive(Debug)]
pub struct BTreeIndex {
  name: String,
  on: String,
  schema: Schema,
  data: BTreeMap<String, csv::StringRecord>,
}

impl BTreeIndex {
  pub fn new(name: &str, on: &str, source: &mut dyn Frame) -> Result<BTreeIndex, error::Error> {
    let schema = source.schema().clone();
    let data = Self::index(&schema, on, source)?;
    Ok(BTreeIndex{
      name: name.to_owned(),
      on: on.to_owned(),
      schema: schema,
      data: data,
    })
  }
  
  fn index(schema: &Schema, on: &str, source: &mut dyn Frame) -> Result<BTreeMap<String, csv::StringRecord>, error::Error> {
    let mut data: BTreeMap<String, csv::StringRecord> = BTreeMap::new();
    
    let index = match schema.index(on) {
      Some(index) => index,
      None => return Err(error::FrameError::new("Index column not found").into()),
    };
    
    for row in source.rows() {
      let row = row?;
      match row.get(index) {
        Some(col) => data.insert(col.to_string(), row),
        None => /* row is omitted */ None,
      };
    }
    
    Ok(data)
  }
}

impl Frame for BTreeIndex {
  fn name<'a>(&'a self) -> &'a str {
    &self.name
  }
  
  fn schema<'a>(&'a self) -> &'a Schema {
    &self.schema
  }
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, error::Error>> + 'a> {
    Box::new(self.data.values().map(|e| { Ok(e.to_owned()) }))
  }
}

impl Index for BTreeIndex {
  fn on<'a>(&'a self) -> &'a str {
    &self.on
  }
  
  fn get<'a>(&'a self, key: &str) -> Result<&'a csv::StringRecord, error::Error> {
    match self.data.get(key) {
      Some(val) => Ok(&val),
      None => Err(error::Error::NotFoundError.into()),
    }
  }
}

impl fmt::Display for BTreeIndex {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.name)
  }
}

// A CSV input frame
#[derive(Debug)]
pub struct Csv<R: io::Read> {
  name: String,
  schema: Schema,
  data: csv::Reader<R>,
}

impl<R: io::Read> Csv<R> {
  pub fn new(name: &str, data: R) -> Result<Csv<R>, error::Error> {
    let mut reader = csv::ReaderBuilder::new().has_headers(false).from_reader(data);
    Ok(Csv{
      name: name.to_owned(),
      schema: Schema::new_from_headers(reader.headers()?),
      data: reader,
    })
  }
}

impl<R: io::Read> Frame for Csv<R> {
  fn name<'a>(&'a self) -> &'a str {
    &self.name
  }
  
  fn schema<'a>(&'a self) -> &'a Schema {
    &self.schema
  }
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, error::Error>> + 'a> {
    Box::new(self.data.records().map(|e| { convert_record(e) }))
  }
}

impl<R: io::Read> fmt::Display for Csv<R> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.name)
  }
}

// A frame that concatenates the output of two other frames
#[derive(Debug)]
pub struct Concat<A: Frame, B: Frame> {
  first: A,
  second: B,
  schema: Schema,
}

impl<A: Frame, B: Frame> Concat<A, B> {
  pub fn new(first: A, second: B) -> Result<Concat<A, B>, error::Error> {
    let s1 = first.schema();
    let s2 = second.schema();
    let schema = s1.union(s2);
    Ok(Concat{
      first: first,
      second: second,
      schema: schema,
    })
  }
}

impl<L: Frame, R: Frame> Frame for Concat<L, R> {
  fn name<'a>(&'a self) -> &'a str {
    self.first.name()
  }
  
  fn schema<'a>(&'a self) -> &'a Schema {
    &self.schema
  }
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, error::Error>> + 'a> {
    Box::new( self.first.rows().chain(self.second.rows()))
  }
}

// A frame that left-joins two frames on an indexed column
#[derive(Debug)]
pub struct Join<L: Frame, R: Index> {
  on: String,
  left: L,
  right: R,
  schema: Schema,
}

impl<L: Frame, R: Index> Join<L, R> {
  pub fn new(on: &str, left: L, right: R) -> Result<Join<L, R>, error::Error> {
    let s1 = left.schema();
    let s2 = right.schema();
    let schema = s1.union(s2);
    Ok(Join{
      on: on.to_string(),
      left: left,
      right: right,
      schema: schema,
    })
  }
}

impl<L: Frame, R: Index> Frame for Join<L, R> {
  fn name<'a>(&'a self) -> &'a str {
    self.left.name()
  }
  
  fn schema<'a>(&'a self) -> &'a Schema {
    &self.schema
  }
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, error::Error>> + 'a> {
    let lname = self.left.name().to_string();
    let index = match self.schema.index(&self.on) {
      Some(index) => index,
      None => return Box::new(iter::once(Err(error::FrameError::new(&format!("Index column not found: {} in {} ({})", &self.on, &lname, &self.schema)).into()))),
    };
    
    let right = &self.right;
    let on = &self.on;
    Box::new(self.left.rows().map(move |row| {
      let row = row?;

      let mut res: Vec<String> = Vec::new();
      for field in &row {
        res.push(field.to_owned()); // can we avoid this?
      }
      
      if let Some(field) = row.get(index) {
        match right.get(&field) {
          Ok(row) => for field in row {
            res.push(field.to_owned()); // can we avoid this?
          },
          Err(err) => match err {
            error::Error::NotFoundError => println!(">>> NOT FOUND: {}={}", on, &field), // not found, do nothing, no join
            err => return Err(err),
          },
        };
      }
      
      Ok(res.into())
    }))
  }
}
