use std::io;
use std::fmt;
use std::cmp;
use std::iter;
use std::collections::BTreeMap;

use csv;

use crate::csvql::query::error;
use crate::csvql::query::select;
use crate::csvql::query::schema;

fn convert_record(e: csv::Result<csv::StringRecord>) -> Result<csv::StringRecord, error::Error> {
  match e {
    Ok(v)    => Ok(v),
    Err(err) => Err(err.into()),
  }
}

// A frame of data
pub trait Frame: fmt::Display {
  fn name<'a>(&'a self) -> &'a str;
  fn schema<'a>(&'a self) -> &'a schema::Schema;
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, error::Error>> + 'a>;
}

impl<F: Frame + ?Sized> Frame for Box<F> { // black magic
  fn name<'a>(&'a self) -> &'a str {
    (**self).name()
  }
  
  fn schema<'a>(&'a self) -> &'a schema::Schema {
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
  schema: schema::Schema,
  data: BTreeMap<String, csv::StringRecord>,
}

impl BTreeIndex {
  pub fn _new(source: &mut dyn Frame, on: &str) -> Result<BTreeIndex, error::Error> {
    let name = source.name().to_owned();
    let schema = source.schema().clone();
    let index_on = schema::QName::new(&name, on);
    let data = Self::_index(&schema, &index_on, source)?;
    Ok(BTreeIndex{
      name: name,
      on: on.to_owned(),
      schema: schema,
      data: data,
    })
  }
  
  fn _index(schema: &schema::Schema, on: &schema::QName, source: &mut dyn Frame) -> Result<BTreeMap<String, csv::StringRecord>, error::Error> {
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
  
  fn schema<'a>(&'a self) -> &'a schema::Schema {
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

// A filtered frame
#[derive(Debug)]
pub struct Filter<F: Frame, S: select::Selector> {
  selector: S,
  data: F,
}

impl<F: Frame, S: select::Selector> Filter<F, S> {
  pub fn new(source: F, selector: S) -> Result<Filter<F, S>, error::Error> {
    Ok(Filter{
      selector: selector,
      data: source,
    })
  }
}

impl<F: Frame, S: select::Selector> Frame for Filter<F, S> {
  fn name<'a>(&'a self) -> &'a str {
    self.data.name()
  }
  
  fn schema<'a>(&'a self) -> &'a schema::Schema {
    self.data.schema()
  }
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, error::Error>> + 'a> {
    let sel = &self.selector;
    Box::new(self.data.rows().map(|e| {
      match e {
        Ok(row) => sel.select(&row),
        Err(err) => Err(err),
      }
    }))
  }
}

impl<F: Frame, S: select::Selector> fmt::Display for Filter<F, S> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}<{}>", self.name(), &self.selector)
  }
}

#[derive(Debug, Eq)]
struct SortedRecord {
  on: String,
  data: csv::StringRecord,
}

impl Ord for SortedRecord {
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    self.on.cmp(&other.on)
  }
}

impl PartialOrd for SortedRecord {
  fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl PartialEq for SortedRecord {
  fn eq(&self, other: &Self) -> bool {
    self.on == other.on
  }
}

// A sorted frame
#[derive(Debug)]
pub struct Sorted {
  name: String,
  on: String,
  schema: schema::Schema,
  data: Vec<SortedRecord>,
}

impl Sorted {
  pub fn new(source: &mut dyn Frame, on: &str) -> Result<Sorted, error::Error> {
    let name = source.name().to_owned();
    let schema = source.schema().clone();
    let index_on = schema::QName::new(&name, on);
    let data = Self::sorted(&schema, &index_on, source)?;
    Ok(Sorted{
      name: name,
      on: on.to_owned(),
      schema: schema,
      data: data,
    })
  }
  
  fn sorted(schema: &schema::Schema, on: &schema::QName, source: &mut dyn Frame) -> Result<Vec<SortedRecord>, error::Error> {
    let index = match schema.index(&on) {
      Some(index) => index,
      None => return Err(error::FrameError::new(&format!("Index column not found: {} ({})", &on, &schema)).into()),
    };
    
    let mut data: Vec<SortedRecord> = Vec::new();
    for row in source.rows() {
      let row = row?;
      let on = match row.get(index) {
        Some(on) => on,
        None => return Err(error::FrameError::new(&format!("Index column not found: {}", index)).into()),
      };
      data.push(SortedRecord{
        on: on.to_owned(),
        data: row,
      });
    }
    
    data.sort();
    Ok(data)
  }
}

impl Frame for Sorted {
  fn name<'a>(&'a self) -> &'a str {
    &self.name
  }
  
  fn schema<'a>(&'a self) -> &'a schema::Schema {
    &self.schema
  }
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, error::Error>> + 'a> {
    Box::new(self.data.iter().map(|e| { Ok(e.data.clone()) }))
  }
}

impl fmt::Display for Sorted {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}{{{}}}", self.name, &self.on)
  }
}

// A CSV input frame
#[derive(Debug)]
pub struct Csv<R: io::Read> {
  name: String,
  schema: schema::Schema,
  data: csv::Reader<R>,
}

impl<R: io::Read> Csv<R> {
  pub fn new(name: &str, data: R) -> Result<Csv<R>, error::Error> {
    let mut reader = csv::ReaderBuilder::new().has_headers(true).from_reader(data);
    Ok(Csv{
      name: name.to_owned(),
      schema: schema::Schema::new(name, reader.headers()?.iter()),
      data: reader,
    })
  }
}

impl<R: io::Read> Frame for Csv<R> {
  fn name<'a>(&'a self) -> &'a str {
    &self.name
  }
  
  fn schema<'a>(&'a self) -> &'a schema::Schema {
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

// A frame that left-joins two frames on an indexed column
#[derive(Debug)]
pub struct Join<L: Frame, R: Index> {
  on: String,
  left: L,
  left_schema: schema::Schema,
  right: R,
  right_schema: schema::Schema,
  join_schema: schema::Schema,
}

impl<L: Frame, R: Index> Join<L, R> {
  pub fn _new(on: &str, left: L, right: R) -> Result<Join<L, R>, error::Error> {
    if right.on() != on {
      return Err(error::FrameError::new(&format!("Join index must use same column as indexed (right) frame: {}", on)).into());
    }
    
    let s1 = left.schema().clone();
    let s2 = right.schema().clone();
    let sjoin = s1.join(&s2._exclude(on));
    
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
  
  fn schema<'a>(&'a self) -> &'a schema::Schema {
    &self.join_schema
  }
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, error::Error>> + 'a> {
    let left_on = schema::QName::new(self.left.name(), &self.on);
    let left_index = match self.left_schema.index(&left_on) {
      Some(index) => index,
      None => return Box::new(iter::once(Err(error::FrameError::new(&format!("Index column not found: {} ({})", &left_on, &self.left_schema)).into()))),
    };
    
    let right = &self.right;
    let right_on = schema::QName::new(self.right.name(), &self.on);
    let right_schema = &self.right_schema;
    let right_index = match self.right_schema.index(&right_on) {
      Some(index) => index,
      None => return Box::new(iter::once(Err(error::FrameError::new(&format!("Index column not found: {} ({})", &right_on, &self.right_schema)).into()))),
    };
    
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
    
    Box::new(rows)
  }
}

impl<L: Frame, R: Index> fmt::Display for Join<L, R> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "({} << {})[{}]", &self.left, &self.right, &self.on)
  }
}

// A frame that outer-joins two frames on a column. Both input
// frames are expected to be sorted by their joining column.
#[derive(Debug)]
pub struct OuterJoin<L: Frame, R: Frame> {
  left: L,
  left_schema: schema::Schema,
  left_on: String,
  
  right: R,
  right_schema: schema::Schema,
  right_on: String,
  
  join_schema: schema::Schema,
}

impl<L: Frame, R: Frame> OuterJoin<L, R> {
  pub fn new(left: L, left_on: &str, right: R, right_on: &str) -> Result<OuterJoin<L, R>, error::Error> {
    let s1 = left.schema().clone();
    let s2 = right.schema().clone();
    let sjoin = s1.join(&s2);
    
    Ok(OuterJoin{
      left: left,
      left_schema: s1,
      left_on: left_on.to_owned(),
      
      right: right,
      right_schema: s2,
      right_on: right_on.to_owned(),
      
      join_schema: sjoin,
    })
  }
}

impl<L: Frame, R: Frame> Frame for OuterJoin<L, R> {
  fn name<'a>(&'a self) -> &'a str {
    self.left.name()
  }
  
  fn schema<'a>(&'a self) -> &'a schema::Schema {
    &self.join_schema
  }
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, error::Error>> + 'a> {
    let left_on = schema::QName::new(self.left.name(), &self.left_on);
    let left_index = match self.left_schema.index(&left_on) {
      Some(index) => index,
      None => return Box::new(iter::once(Err(error::FrameError::new(&format!("Index column not found: {} ({})", &left_on, &self.left_schema)).into()))),
    };
    
    let right_on = schema::QName::new(self.right.name(), &self.right_on);
    let right_index = match self.right_schema.index(&right_on) {
      Some(index) => index,
      None => return Box::new(iter::once(Err(error::FrameError::new(&format!("Index column not found: {} ({})", &right_on, &self.right_schema)).into()))),
    };
    
    let mut rows: Vec<Result<csv::StringRecord, error::Error>> = Vec::new();
    
    let mut iter_left = self.left.rows();
    let mut curr_left: Option<Result<csv::StringRecord, error::Error>> = None;
    let mut iter_right = self.right.rows();
    let mut curr_right: Option<Result<csv::StringRecord, error::Error>> = None;
    
    loop {
      curr_left = match curr_left {
        Some(val) => Some(val),
        None => iter_left.next(),
      };
      let (left, left_cmp) = match &curr_left {
        Some(left) => match left {
          Ok(left) => match left.get(left_index) {
            Some(cmp) => (Some(left), Some(cmp)),
            None => (Some(left), None),
          },
          Err(err) => {
            rows.push(Err(error::FrameError::new(&format!("Error reading left side of join: {}", err)).into()));
            break;
          },
        },
        None => (None, None),
      };
      
      curr_right = match curr_right {
        Some(val) => Some(val),
        None => iter_right.next(),
      };
      let (right, right_cmp) = match &curr_right {
        Some(right) => match right {
          Ok(right) => match right.get(right_index) {
            Some(cmp) => (Some(right), Some(cmp)),
            None => (Some(right), None),
          },
          Err(err) => {
            rows.push(Err(error::FrameError::new(&format!("Error reading right side of join: {}", err)).into()));
            break;
          },
        },
        None => (None, None),
      };
      
      let mut row: Vec<String> = Vec::new();
      if let (Some(left_cmp), Some(right_cmp)) = (left_cmp, right_cmp) {
        if left_cmp < right_cmp {
          row.extend(&mut left.unwrap().iter().map(|e| { e.to_owned() }));
          row.append(&mut self.right_schema.empty_row(0));
          curr_left = None;
        }else if right_cmp < left_cmp {
          row.append(&mut self.left_schema.empty_row(0));
          row.extend(&mut right.unwrap().iter().map(|e| { e.to_owned() }));
          curr_right = None;
        }else{
          row.extend(&mut left.unwrap().iter().map(|e| { e.to_owned() }));
          curr_left = None;
          row.extend(&mut right.unwrap().iter().map(|e| { e.to_owned() }));
          curr_right = None;
        }
      }else if let Some(_) = left_cmp {
        row.extend(&mut left.unwrap().iter().map(|e| { e.to_owned() }));
        row.append(&mut self.right_schema.empty_row(0));
        curr_left = None;
      }else if let Some(_) = right_cmp {
        row.append(&mut self.left_schema.empty_row(0));
        row.extend(&mut right.unwrap().iter().map(|e| { e.to_owned() }));
        curr_right = None;
      }else{
        break; // no data left; done processing
      }
      
      rows.push(Ok(row.into()));
    }
    
    Box::new(rows.into_iter())
  }
}

impl<L: Frame, R: Frame> fmt::Display for OuterJoin<L, R> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "({} <> {})[{}, {}]", &self.left, &self.right, &self.left_on, &self.right_on)
  }
}
