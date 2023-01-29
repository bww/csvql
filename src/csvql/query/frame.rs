use std::io;
use std::fmt;
use std::cmp;
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

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QName {
  scope: String,
  name: String,
}

impl QName {
  pub fn new(scope: &str, name: &str) -> QName {
    QName{
      scope: scope.to_owned(),
      name: name.to_owned(),
    }
  }
  
  pub fn format(scope: &str, name: &str) -> String {
    format!("{}.{}", scope, name)
  }
  
  pub fn _scope<'a>(&'a self) -> &'a str {
    &self.scope
  }
  
  pub fn name<'a>(&'a self) -> &'a str {
    &self.name
  }
  
  pub fn qname(&self) -> String {
    Self::format(&self.scope, &self.name)
  }
}

impl fmt::Display for QName {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", &self.qname())
  }
}

#[derive(Clone)]
pub struct Schema {
  cmap: HashMap<QName, usize>,
  keys: Vec<QName>,
}

impl Schema {
  pub fn new<'a>(scope: &str, hdrs: impl iter::Iterator<Item=&'a str>) -> Schema {
    Self::new_with_keys(hdrs.map(|e| { QName::new(scope, e) }).collect())
  }
  
  fn new_with_keys<'a>(mut keys: Vec<QName>) -> Schema {
    keys.sort();
    
    let mut cmap: HashMap<QName, usize> = HashMap::new();
    for (i, k) in keys.iter().enumerate() {
      cmap.insert(k.clone(), i);
    }
    
    Schema{
      cmap: cmap,
      keys: keys,
    }
  }
  
  pub fn union(&self, with: &Schema) -> Schema {
    let mut cols: HashSet<QName> = HashSet::new();
    for (k, _) in &self.cmap {
      cols.insert(k.to_owned());
    }
    for (k, _) in &with.cmap {
      cols.insert(k.to_owned());
    }
    
    let mut keys: Vec<QName> = Vec::new();
    for k in cols.into_iter() {
      keys.push(k);
    }
    
    keys.sort();
    
    let mut cmap: HashMap<QName, usize> = HashMap::new();
    for (i, k) in keys.iter().enumerate() {
      cmap.insert(k.clone(), i);
    }
    
    Schema{
      cmap: cmap,
      keys: keys,
    }
  }
  
  pub fn exclude(&self, col: &str) -> Schema {
    Self::new_with_keys(self.keys.iter().filter(|e| { e.name() != col }).map(|e| { e.clone() }).collect())
  }
  
  pub fn count(&self) -> usize {
    self.keys.len()
  }
  
  pub fn empty_row(&self, adjust: i32) -> Vec<String> {
    return Self::empty_vec((self.count() as i32 + cmp::max(-(self.count() as i32), adjust)) as usize);
  }
  
  pub fn _get<'a>(&'a self, name: &str) -> Option<&'a QName> {
    for e in &self.keys {
      if e.name() == name {
        return Some(e)
      }
    }
    None
  }
  
  pub fn columns<'a>(&'a self) -> Vec<&'a QName> {
    self.keys.iter().collect()
  }
  
  pub fn record<'a>(&'a self) -> Vec<&'a str> {
    self.keys.iter().map(|e| { e.name() }).collect()
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
        dsc.push_str(&key.name());
      }
      n += 1;
    }
    dsc
  }
  
  pub fn index(&self, qname: &QName) -> Option<usize> {
    match self.cmap.get(qname) {
      Some(index) => Some(*index),
      None => None,
    }
  }
  
  fn empty_vec(count: usize) -> Vec<String> {
    let mut empty: Vec<String> = Vec::new();
    for _ in 0..count {
      empty.push(String::new());
    }
    empty
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
pub trait Frame: fmt::Display {
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
  pub fn new(source: &mut dyn Frame, on: &str) -> Result<BTreeIndex, error::Error> {
    let name = source.name().to_owned();
    let schema = source.schema().clone();
    let index_on = QName::new(&name, on);
    let data = Self::index(&schema, &index_on, source)?;
    Ok(BTreeIndex{
      name: name,
      on: on.to_owned(),
      schema: schema,
      data: data,
    })
  }
  
  fn index(schema: &Schema, on: &QName, source: &mut dyn Frame) -> Result<BTreeMap<String, csv::StringRecord>, error::Error> {
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
    write!(f, "{}[{}]", self.name, &self.on)
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
    let mut reader = csv::ReaderBuilder::new().has_headers(true).from_reader(data);
    Ok(Csv{
      name: name.to_owned(),
      schema: Schema::new(name, reader.headers()?.iter()),
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

impl<A: Frame, B: Frame> fmt::Display for Concat<A, B> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "({} + {})", &self.first, &self.second)
  }
}

// A frame that left-joins two frames on an indexed column
#[derive(Debug)]
pub struct Join<L: Frame, R: Index> {
  on: String,
  left: L,
  left_schema: Schema,
  right: R,
  right_schema: Schema,
  join_schema: Schema,
}

impl<L: Frame, R: Index> Join<L, R> {
  pub fn new(on: &str, left: L, right: R) -> Result<Join<L, R>, error::Error> {
    if right.on() != on {
      return Err(error::FrameError::new(&format!("Join index must use same column as indexed (right) frame: {}", on)).into());
    }
    
    let s1 = left.schema().clone();
    let s2 = right.schema().clone();
    let sjoin = s1.union(&s2.exclude(on));
    
    Ok(Join{
      on: on.to_string(),
      left: left,
      left_schema: s1,
      right: right,
      right_schema: s2,
      join_schema: sjoin,
    })
  }
}

impl<L: Frame, R: Index> Frame for Join<L, R> {
  fn name<'a>(&'a self) -> &'a str {
    self.left.name()
  }
  
  fn schema<'a>(&'a self) -> &'a Schema {
    &self.join_schema
  }
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, error::Error>> + 'a> {
    let left_on = QName::new(self.left.name(), &self.on);
    let left_index = match self.left_schema.index(&left_on) {
      Some(index) => index,
      None => return Box::new(iter::once(Err(error::FrameError::new(&format!("Index column not found: {} ({})", &left_on, &self.left_schema)).into()))),
    };
    
    let right = &self.right;
    let right_on = QName::new(self.right.name(), &self.on);
    let right_schema = &self.right_schema;
    let right_index = match self.right_schema.index(&right_on) {
      Some(index) => index,
      None => return Box::new(iter::once(Err(error::FrameError::new(&format!("Index column not found: {} ({})", &right_on, &self.right_schema)).into()))),
    };
    
    // let hdrs = iter::once::<Result<csv::StringRecord, error::Error>>(
    //   Ok(self.join_schema.columns().iter().map(|e| { e.name() }).collect::<Vec<&str>>().into())
    // );
    let rows = self.left.rows().map(move |row| {
      let row = row?;
      
      let mut res: Vec<String> = Vec::new();
      for field in &row {
        res.push(field.to_owned()); // can we avoid this copy?
      }
      
      if let Some(field) = row.get(left_index) {
        match right.get(&field) {
          Ok(row) => for (i, field) in row.iter().enumerate() {
            if i != right_index {
              res.push(field.to_owned()); // can we avoid this copy?
            }
          },
          Err(err) => match err {
            error::Error::NotFoundError => res.append(&mut right_schema.empty_row(-1)),
            err => return Err(err),
          },
        };
      }
      
      Ok(res.into())
    });
    
    // Box::new(hdrs.chain(rows))
    Box::new(rows)
  }
}

impl<L: Frame, R: Index> fmt::Display for Join<L, R> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "({} <> {})[{}]", &self.left, &self.right, &self.on)
  }
}
