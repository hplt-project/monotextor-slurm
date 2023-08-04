use std::io::Write;
use std::fs;
use tempfile::NamedTempFile;
use assert_cmd::Command;
use zstd::bulk::compress;

// Test that checks index and dedup of a sample of documents
#[test]
fn mhindex_dedup() -> Result<(), Box<dyn std::error::Error>> {
    let expected_output = fs::read_to_string("tests/dedup.out")?;
    let mut mhindex_cmd = Command::cargo_bin("mhindex")?;

    let mhindex_out = mhindex_cmd.arg("tests/sample.jsonl.zst")
        .output().unwrap().stdout;
    let mut temp = NamedTempFile::new()?;
    temp.write_all(&compress(&mhindex_out, 0)?)?;

    let mut dedup_cmd = Command::cargo_bin("dedup")?;
    dedup_cmd
        .arg(temp.path())
        .arg("tests/sample.jsonl.zst")
        .assert()
        .success()
        .stdout(expected_output);

    Ok(())
}


#[test]
fn tsv2jsonl() -> Result<(), Box<dyn std::error::Error>> {
    let expected_output = fs::read_to_string("tests/example.jsonl")?;
    let mut cmd = Command::cargo_bin("tsv2jsonl")?;

    cmd
        .arg("-l")
        .arg("fi")
        .pipe_stdin("tests/example.tsv")?
        .assert()
        .success()
        .stdout(expected_output);

    Ok(())
}
