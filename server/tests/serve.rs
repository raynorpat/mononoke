// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn sibling_bin(name: &str) -> PathBuf {
    let mononoke = PathBuf::from(env!("CARGO_BIN_EXE_mononoke"));
    let mut path = mononoke
        .parent()
        .unwrap_or_else(|| panic!("no directory for {}", mononoke.display()))
        .to_path_buf();
    let file = if cfg!(windows) {
        format!("{name}.exe")
    } else {
        name.to_string()
    };
    path.push(file);
    path
}

fn hg(repo: &Path, args: &[&str]) -> Output {
    let mut cmd = Command::new("hg");
    cmd.current_dir(repo)
        .args(args)
        .env("HGPLAIN", "1")
        .env("HGRCPATH", "");
    output_timeout(&mut cmd, Duration::from_secs(120))
}

fn output_timeout(cmd: &mut Command, timeout: Duration) -> Output {
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd
        .spawn()
        .unwrap_or_else(|err| panic!("failed to spawn {cmd:?}: {err}"));
    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return child.wait_with_output().expect("wait for child"),
            Ok(None) if started.elapsed() > timeout => {
                let _ = child.kill();
                let output = child.wait_with_output().expect("wait for killed child");
                panic!(
                    "timed out after {}s: {cmd:?}\nstdout:\n{}\nstderr:\n{}",
                    timeout.as_secs(),
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            Ok(None) => thread::sleep(Duration::from_millis(50)),
            Err(err) => panic!("wait failed for {cmd:?}: {err}"),
        }
    }
}

fn assert_hg(output: &Output, what: &str) {
    assert!(
        output.status.success(),
        "{what} failed: status {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn init_zstd_repo(root: &Path) -> PathBuf {
    let repo = root.join("repo");
    let mut init = Command::new("hg");
    init.current_dir(root)
        .args(["init", "repo"])
        .args(["--config", "format.generaldelta=true"])
        .args(["--config", "format.revlog-compression=zstd"])
        .args(["--config", "format.sparse-revlog=false"])
        .args(["--config", "format.use-persistent-nodemap=false"])
        .args(["--config", "format.use-share-safe=false"])
        .args(["--config", "format.use-dirstate-v2=false"])
        .args([
            "--config",
            "format.exp-use-copies-side-data-changeset=false",
        ])
        .env("HGPLAIN", "1")
        .env("HGRCPATH", "");
    let init = output_timeout(&mut init, Duration::from_secs(120));
    assert_hg(&init, "hg init");

    fs::write(repo.join("a.txt"), b"alpha\n").expect("write a.txt");
    assert_hg(&hg(&repo, &["add", "a.txt"]), "hg add");
    assert_hg(
        &hg(&repo, &["commit", "-m", "first", "-u", "test"]),
        "hg commit",
    );

    let requires_path = repo.join(".hg").join("requires");
    let requires = fs::read_to_string(&requires_path).unwrap_or_else(|err| {
        panic!("read {}: {err}", requires_path.display());
    });
    let allowed = [
        "store",
        "fncache",
        "dotencode",
        "generaldelta",
        "revlogv1",
        "revlog-compression-zstd",
        "usefncache",
    ];
    for line in requires.lines() {
        assert!(
            allowed.contains(&line),
            "unexpected requires line {line:?} in:\n{requires}"
        );
    }
    repo
}

fn write_ssh_shim(dir: &Path, hgcli: &Path, repo: &Path) -> PathBuf {
    if cfg!(windows) {
        let path = dir.join("ssh.cmd");
        let script = format!(
            "@echo off\r\n\"{}\" -R \"{}\" serve --stdio\r\n",
            hgcli.display(),
            repo.display()
        );
        fs::write(&path, script).expect("write ssh shim");
        path
    } else {
        let path = dir.join("ssh.sh");
        let script = format!(
            "#!/bin/sh\nexec \"{}\" -R \"{}\" serve --stdio\n",
            hgcli.display(),
            repo.display()
        );
        fs::write(&path, script).expect("write ssh shim");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&path).expect("shim metadata").permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&path, perms).expect("shim permissions");
        }
        path
    }
}

fn cfg_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("m{label}{nanos}"));
        fs::create_dir_all(&path).expect("temp dir");
        TempDir(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Running {
    child: Child,
}

impl Running {
    fn start(repo: &Path, log: &Path) -> Self {
        let bin = sibling_bin("mononoke");
        let log_file =
            File::create(log).unwrap_or_else(|err| panic!("log {}: {err}", log.display()));
        let stderr = log_file
            .try_clone()
            .unwrap_or_else(|err| panic!("clone log handle: {err}"));
        let mut child = Command::new(&bin)
            .arg(repo)
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(stderr))
            .spawn()
            .unwrap_or_else(|err| panic!("spawn {}: {err}", bin.display()));
        let sock = repo.join(".hg").join("mononoke.sock");
        let started = Instant::now();
        loop {
            if sock.exists() {
                return Running { child };
            }
            match child.try_wait() {
                Ok(Some(status)) => {
                    panic!(
                        "mononoke exited {status} before {}\n{}",
                        sock.display(),
                        read_log(log)
                    );
                }
                Ok(None) if started.elapsed() > Duration::from_secs(15) => {
                    let _ = child.kill();
                    panic!(
                        "timed out waiting for {}\n{}",
                        sock.display(),
                        read_log(log)
                    );
                }
                Ok(None) => thread::sleep(Duration::from_millis(50)),
                Err(err) => panic!("mononoke wait failed: {err}\n{}", read_log(log)),
            }
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn read_log(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|_| String::new())
}

fn hg_ssh(dir: &Path, args: &[&str], shim: &Path) -> Output {
    let mut cmd = Command::new("hg");
    cmd.current_dir(dir)
        .args(args)
        .arg("--config")
        .arg(format!("ui.ssh={}", cfg_path(shim)))
        .env("HGPLAIN", "1")
        .env("HGRCPATH", "");
    output_timeout(&mut cmd, Duration::from_secs(120))
}

#[test]
fn ssh_clone_has_file_contents() {
    let temp = TempDir::new("c");
    let repo = init_zstd_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let _server = Running::start(&repo, &log);
    let shim = write_ssh_shim(&temp.0, &sibling_bin("hgcli"), &repo);
    let clone = hg_ssh(&temp.0, &["clone", "ssh://user@dummy/repo", "dest"], &shim);
    assert!(
        clone.status.success(),
        "hg clone failed: status {:?}\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        clone.status,
        String::from_utf8_lossy(&clone.stdout),
        String::from_utf8_lossy(&clone.stderr),
        read_log(&log)
    );
    let dest = temp.0.join("dest");
    let body = fs::read(dest.join("a.txt")).unwrap_or_else(|err| {
        panic!(
            "read dest a.txt: {err}\nclone stdout:\n{}\nclone stderr:\n{}\nmononoke:\n{}",
            String::from_utf8_lossy(&clone.stdout),
            String::from_utf8_lossy(&clone.stderr),
            read_log(&log)
        )
    });
    assert_eq!(
        body,
        b"alpha\n",
        "a.txt bytes {body:?}\nmononoke:\n{}",
        read_log(&log)
    );
    let logged = hg(&dest, &["log", "-T", "{desc}"]);
    assert_hg(&logged, "hg log");
    let text = String::from_utf8_lossy(&logged.stdout);
    assert!(
        text.contains("first"),
        "log {text:?} does not contain first"
    );
}

#[test]
fn ssh_pull_learns_bookmark_without_new_changesets() {
    let temp = TempDir::new("p");
    let repo = init_zstd_repo(&temp.0);
    let local = hg(&temp.0, &["clone", "repo", "dest"]);
    assert_hg(&local, "filesystem clone");

    let log = temp.0.join("mononoke.log");
    let _server = Running::start(&repo, &log);
    assert_hg(&hg(&repo, &["bookmark", "test-bookmark"]), "hg bookmark");

    let shim = write_ssh_shim(&temp.0, &sibling_bin("hgcli"), &repo);
    let dest = temp.0.join("dest");
    let pull = hg_ssh(&dest, &["pull", "ssh://user@dummy/repo"], &shim);
    assert!(
        pull.status.success(),
        "hg pull failed: status {:?}\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        pull.status,
        String::from_utf8_lossy(&pull.stdout),
        String::from_utf8_lossy(&pull.stderr),
        read_log(&log)
    );

    let books = hg(&dest, &["bookmarks"]);
    assert_hg(&books, "hg bookmarks");
    let books_text = String::from_utf8_lossy(&books.stdout);
    assert!(
        books_text.contains("test-bookmark"),
        "bookmarks {books_text:?}\nmononoke:\n{}",
        read_log(&log)
    );

    let logged = hg(&dest, &["log", "-T", "{rev}\n"]);
    assert_hg(&logged, "hg log");
    let count = String::from_utf8_lossy(&logged.stdout)
        .lines()
        .filter(|line| !line.is_empty())
        .count();
    assert_eq!(
        count,
        1,
        "dest changelog has {count} changesets:\n{}",
        String::from_utf8_lossy(&logged.stdout)
    );
}
