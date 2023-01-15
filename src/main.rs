mod csvql;

use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
pub struct Options {
  #[clap(long, help="Enable debugging mode")]
  pub debug: bool,
  #[clap(long, help="Enable alternate screen debugging mode (no switch on exit)")]
  pub debug_alternate: bool,
  #[clap(long, help="Enable editor debugging mode; additional frames are not displayed")]
  pub debug_editor: bool,
  #[clap(long)]
  pub verbose: bool,
  #[clap(help="Document to open")]
  pub doc: Option<String>,
}

fn main() {
  println!("Hello, world!");
}
