use std::env;
use monotextor_utils::zpaste::ZPaste;

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let mf = ZPaste::new(args);
    for i in mf {
        println!("{}", i);
    }
}
