mod csvql;
mod error;

use std::io;
use std::fs;
use std::process;

use clap::Parser;

use csvql::query;
use csvql::query::frame;
use csvql::query::frame::Frame;
// use csvql::query::select;
// use csvql::query::select::Selector;

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
pub struct Options {
  #[clap(long, help="Enable debugging mode")]
  pub debug: bool,
  #[clap(long)]
  pub verbose: bool,
  #[clap(long, help="Join inputs on the specified column")]
  pub join: Option<String>,
  #[clap(long, help="Select columns")]
  pub select: Vec<String>,
  #[clap(help="Document to open")]
  pub docs: Vec<String>,
}

fn main() {
  match cmd() {
    Ok(_)     => {},
    Err(err)  => {
      eprintln!("* * * {}", err);
      process::exit(1);
    },
  };
}

fn cmd() -> Result<(), error::Error> {
  let opts = Options::parse();
  
  let mut frms: Vec<Box<dyn Frame>> = Vec::new();
  for s in &opts.docs {
    let (name, input): (&str, Box<dyn io::Read>) = if s == "-" {
      ("-", Box::new(io::stdin()))
    }else{
      let (alias, path) = parse_source(&s);
      (alias, Box::new(fs::File::open(path)?))
    };
    frms.push(Box::new(query::frame::Csv::new(&name, input)?));
  }
  
  // let mut cols: Vec<select::Column> = Vec::new();
  // for s in &opts.select {
  //   cols.push(select::Column::parse(&s)?);
  // }
  
  let mut frms = if let Some(on) = &opts.join {
    let mut base: Option<Box<dyn Frame>> = None;
    for mut frm in frms.into_iter() {
      if let Some(curr) = base {
        base = Some(Box::new(frame::Join::new(on, curr, frame::BTreeIndex::new(&mut frm, on)?)?));
      }else{
        base = Some(Box::new(frm));
      }
    }
    if let Some(base) = base {
      vec![base]
    }else{
      Vec::new()
    }
  }else{
    frms
  };
  
  // let sel = select::Join::new_with_columns(cols);
  for frm in frms.iter_mut() {
    eprintln!(">>> {}", frm);
    let mut dst = csv::Writer::from_writer(io::stdout());
    dst.write_record(frm.schema().record())?;
    
    // let mut it = frm.rows();
    // let schema = if let Some(hdrs) = it.next() {
    //   let hdrs = hdrs?;
    //   let schema = frame::Schema::new_from_headers(&hdrs);
    //   dst.write_record(&sel.select(&schema, &hdrs)?)?;
    //   schema
    // }else{
    //   break;
    // };
    for r in frm.rows() {
      // let r = sel.select(&schema, &r?)?;
      let r = r?;
      dst.write_record(&r)?;
    }
    
    dst.flush()?;
  }
  
  Ok(())
}

fn parse_source<'a>(f: &'a str) -> (&'a str, &'a str) {
  let split: Vec<&'a str> = f.splitn(2, "=").collect();
  match split.len() {
    2 => (split[0], split[1]),
    1 => (split[0], split[0]),
    _ => ("", ""),
  }
}
