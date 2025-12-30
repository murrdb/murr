mod conf;
mod core;

fn main() {
    let cnf = conf::Config::from_str("asd");
    println!("Hello, world!");
}
