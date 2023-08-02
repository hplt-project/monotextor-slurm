use std::env;
use monotextor_utils::queryreader::QueryReader;

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let mf = QueryReader::new(args);
    for i in mf {
        println!("{}", i);
    }
}
