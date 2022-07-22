use bytes::Buf;
use bytes::Bytes;
use std::fs::{read_dir, read_to_string};
use std::io::BufRead;
use std::path::Path;

pub fn walk<F>(path: &str, mut f: F)
where
    F: FnMut(&str),
{
    for entry in read_dir(path).unwrap() {
        let path = entry.unwrap().path();
        if !path.is_file() {
            //if !path.is_file() || !path.ends_with("joint_commit.txt") {
            continue;
        }
        f(path.to_str().unwrap())
    }
}

pub fn execute_test<P: AsRef<Path>, F>(path: P, split: &str, mut f: F)
where
    F: FnMut(&TestData) -> String,

{
    use bytes::Buf;
    let mut data = vec![];
    let txt = read_to_string(path).unwrap();
    let lines = txt.split(split).collect::<Vec<_>>();
    let mut print_buf = vec![];
    for line in lines {
        let mut rd = Bytes::from(line.to_string()).reader();
        let mut buf = String::new();
        let mut cmd = TestData::default();
        while let Ok(n) = rd.read_line(&mut buf) {
            if n == 0 {
                break;
            }
            if buf.starts_with("#") {
                buf.clear();
                continue;
            }
            buf = buf.trim_end().to_string();
            if buf.len() == 0 {
                buf.clear();
                continue;
            }
            if buf.starts_with("title: ") {
                cmd.title = buf.as_str()["title: ".len()..].to_string();
            } else if buf.starts_with("cmd: ") {
                cmd.cmd = buf.as_str()["cmd: ".len()..].to_string();
            } else if buf.starts_with("args: ") {
                let args = buf.as_str()["args: ".len()..].to_string();
                for arg in args.split_terminator(" ").collect::<Vec<_>>() {
                    let mut cmd_arg = CmdArg {
                        key: "".to_string(),
                        vals: vec![],
                    };
                    let arg = arg.split("=").collect::<Vec<_>>();
                    cmd_arg.key = arg[0].to_string();
                    cmd_arg.vals = arg[1]
                        .trim_start_matches('(')
                        .trim_end_matches(')')
                        .split(",")
                        .filter(|s| s.trim() != "")
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>();
                    cmd.cmd_args.push(cmd_arg);
                }
            } else if buf.starts_with("output:") {
            } else {
                cmd.output.push_str(buf.as_str());
                cmd.output.push_str("\n");
            }
            buf.clear();
        }
        cmd.output = cmd.output.trim_end().to_string();
        // println!("title: {}, cmd: {}, args: {:?}, output: {}", cmd.title, cmd.cmd, cmd.cmd_args, cmd.output);
        data.push(cmd);
        print_buf.push(line);
    }

    for (i, datum) in data.iter_mut().enumerate() {
        println!("t_{}", i);
        println!("{}", print_buf[i]);
        println!("{:?}", datum);
        assert_eq!(f(datum), datum.output);
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct TestData {
    pub title: String,
    pub cmd: String,
    pub cmd_args: Vec<CmdArg>,
    pub output: String,
}

#[derive(Debug, Default, PartialEq)]
pub struct CmdArg {
    pub key: String,
    pub vals: Vec<String>,
}
