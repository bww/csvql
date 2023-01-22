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
  
  let mut dst = csv::Writer::from_writer(io::stdout());
  let sel = select::Join::new_with_columns(cols);
  for frm in frms.iter_mut() {
    let mut it = frm.rows();
    let schema = if let Some(hdrs) = it.next() {
      let hdrs = hdrs?;
      let schema = select::Schema::new_from_headers(&hdrs);
      dst.write_record(&sel.select(&schema, &hdrs)?)?;
      schema
    }else{
      break;
    };
    for r in it {
      let r = sel.select(&schema, &r?)?;
      dst.write_record(&r)?;
    }
  }
  
  dst.flush()?;
  Ok(())
}
