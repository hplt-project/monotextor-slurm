use log::{debug, info, warn};
use std::process::{id, Command};
use std::str::from_utf8;

pub fn memory_usage() {
    let cmd_out = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "cat /proc/{}/status | grep -m 1 VmHWM | grep -o '[0-9]*'",
            id()
        ))
        .output();
    if let Err(err) = cmd_out {
        warn!("Could not obtain memory usage");
        debug!("{}", err);
    } else if let Ok(output) = cmd_out {
        let mem = from_utf8(&output.stdout)
            .expect("Error decoding command output")
            .strip_suffix("\n")
            .unwrap()
            .to_string()
            .parse::<u32>()
            .unwrap() as f32
            / 1e6;
        info!("Peak memory used: {:.2} GB", mem);
    }
}
