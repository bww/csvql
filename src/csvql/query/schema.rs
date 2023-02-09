use std::fmt;
use std::cmp;
use std::iter;
use std::hash;
use std::collections::HashMap;

use crate::csvql::query::error;

#[derive(Clone, Eq)]
pub struct QName {
  scope: Option<String>,
  name: String,
}

impl QName {
  pub fn parse(text: &str) -> Result<QName, error::Error> {
    let split: Vec<&str> = text.splitn(2, ".").collect();
    match split.len() {
      2 => Ok(Self::new(split[0], split[1])),
      1 => Ok(Self::new_unscoped(split[0])),
      _ => Err(error::ParseError::new(&format!("Invalid qname format: {}", text)).into()),
    }
  }
  
  pub fn new_unscoped(name: &str) -> QName {
    QName{
      scope: None,
      name: name.to_owned(),
    }
  }
  
  pub fn new(scope: &str, name: &str) -> QName {
    QName{
      scope: Some(scope.to_owned()),
      name: name.to_owned(),
    }
  }
  
  pub fn format(scope: Option<&str>, name: &str) -> String {
    match scope {
      Some(scope) => format!("{}.{}", scope, name),
      None => name.to_string(),
    }
  }
  
  pub fn name<'a>(&'a self) -> &'a str {
    &self.name
  }
  
  pub fn scope<'a>(&'a self) -> Option<&'a str> {
    match &self.scope {
      Some(scope) => Some(scope),
      None => None,
    }
  }
  
  pub fn qname(&self) -> String {
    Self::format(self.scope(), &self.name)
  }
  
  pub fn matches(&self, other: &Self) -> bool {
    match &self.scope {
      Some(_) => self.scope.cmp(&other.scope).then(self.name.cmp(&other.name)) == cmp::Ordering::Equal,
      None => self.name.cmp(&other.name) == cmp::Ordering::Equal,
    }
  }
}

impl Ord for QName {
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    self.scope.cmp(&other.scope).then(self.name.cmp(&other.name))
  }
}

impl PartialOrd for QName {
  fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl PartialEq for QName {
  fn eq(&self, other: &Self) -> bool {
    self.scope == other.scope && self.name == other.name
  }
}

impl hash::Hash for QName {
  fn hash<H: hash::Hasher>(&self, state: &mut H) {
    match &self.scope {
      Some(scope) => scope.hash(state),
      None => {},
    };
    self.name.hash(state);
  }
}

impl fmt::Display for QName {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", &self.qname())
  }
}

impl fmt::Debug for QName {
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
  
  fn new_with_keys<'a>(keys: Vec<QName>) -> Schema {
    let mut cmap: HashMap<QName, usize> = HashMap::new();
    for (i, k) in keys.iter().enumerate() {
      cmap.insert(k.clone(), i);
    }
    
    Schema{
      cmap: cmap,
      keys: keys,
    }
  }
  
  pub fn join(&self, with: &Schema) -> Schema {
    let mut keys: Vec<QName> = Vec::new();
    for k in &self.keys {
      keys.push(k.clone());
    }
    for k in &with.keys {
      keys.push(k.clone());
    }
    
    let mut cmap: HashMap<QName, usize> = HashMap::new();
    for (i, k) in keys.iter().enumerate() {
      cmap.insert(k.clone(), i);
    }
    
    Schema{
      cmap: cmap,
      keys: keys,
    }
  }
  
  pub fn _exclude(&self, col: &str) -> Schema {
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
  
  pub fn _columns<'a>(&'a self) -> Vec<&'a QName> {
    self.keys.iter().collect()
  }
  
  pub fn record<'a>(&'a self) -> Vec<String> {
    self.keys.iter().map(|e| { e.qname() }).collect()
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
    if let Some(_) = qname.scope() {
      return match self.cmap.get(qname) {
        Some(index) => Some(*index),
        None => None,
      };
    }else{
      for (i, e) in self.keys.iter().enumerate() {
        if qname.matches(e) {
          return Some(i);
        }
      }
    }
    None
  }
  
  pub fn indexes(&self, qnames: &Vec<QName>) -> Option<Vec<usize>> {
    let mut indexes: Vec<usize> = Vec::new();
    for n in qnames {
      if let Some(x) = self.index(n) {
        indexes.push(x);
      }else{
        return None;
      }
    }
    Some(indexes)
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
