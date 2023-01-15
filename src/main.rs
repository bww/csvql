mod csvql;

use std::io;
use std::fs;

use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
pub struct Options {
  #[clap(long, help="Enable debugging mode")]
  pub debug: bool,
  #[clap(long)]
  pub verbose: bool,
  #[clap(help="Document to open")]
  pub docs: Vec<String>,
}

fn main() {
  let opts = Options::parse();
  println!("Hello, world! {:?}", opts);
  
  let mut srcs: Vec<csvql::Source<Box<dyn io::Read>>> = Vec::new();
  for s in opts.docs {
    srcs.push(Box::new(io::stdin()));
  }
  
  let mut sels: Vec<csvql::Selector<Box<dyn io::Read>>> = Vec::new();
  
  let query = csvql::Query::new(srcs, selectors);
  
}
