mod csvql;
mod error;

use std::io;
use std::fs;
use std::process;

use clap::Parser;

use csvql::query;
use csvql::query::frame::Frame;
use csvql::query::select;
use csvql::query::select::Selector;

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
pub struct Options {
  #[clap(long, help="Enable debugging mode")]
  pub debug: bool,
  #[clap(long)]
  pub verbose: bool,
  #[clap(long, help="Select columns")]
  pub select: Vec<String>,
  #[clap(help="Document to open")]
  pub docs: Vec<String>,
}

fn main() {
  match cmd() {
    Ok(_)     => {},
    Err(err)  => {
      println!("* * * {}", err);
      process::exit(1);
    },
  };
}

fn cmd() -> Result<(), error::Error> {
  let opts = Options::parse();
  println!("Hello, world! {:?}", opts);
  
  let mut frms: Vec<query::frame::Csv<Box<dyn io::Read>>> = Vec::new();
  for s in &opts.docs {
    let (name, input): (&str, Box<dyn io::Read>) = if s == "-" {
      ("-", Box::new(io::stdin()))
    }else{
      (&s, Box::new(fs::File::open(&s)?))
    };
    frms.push(query::frame::Csv::new(&name, input));
  }
  
  let mut cols: Vec<select::Column> = Vec::new();
  for s in &opts.select {
    cols.push(select::Column::parse(&s)?);
  }
  
  let sel = select::Join::new_with_columns(cols);
  for frm in frms.iter_mut() {
    println!(">>> {}", frm);
    let mut it = frm.rows();
    let schema = if let Some(hdrs) = it.next() {
      select::Schema::new_from_headers(&hdrs?)
    }else{
      break;
    };
    for r in it {
      let r = sel.select(&schema, &r?)?;
      // let pos = r.position().expect("a record position");
      println!(">>> {:?}", r);
    }
  }
  
  // let mut query = query::Query::new(srcs, sels);
  
  Ok(())
}
