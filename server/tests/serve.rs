// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Serializes ephemeral HTTP port allocation so parallel tests do not bind the same port.
static HTTP_BIND: Mutex<()> = Mutex::new(());

fn lock_ports() -> std::sync::MutexGuard<'static, ()> {
    HTTP_BIND.lock().unwrap_or_else(|err| err.into_inner())
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("ephemeral port");
    listener.local_addr().expect("ephemeral addr").port()
}

fn free_port_except(taken: u16) -> u16 {
    loop {
        let port = free_port();
        if port != taken {
            return port;
        }
    }
}

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

/// Share-safe sparse revlog v1 repo with two revisions of `a.txt`.
///
/// Leaves the revlog Mercurial wrote. A self-based filelog snapshot is not
/// rewritten into a delta.
fn init_share_safe_repo(root: &Path) -> PathBuf {
    let repo = root.join("repo");
    let mut init = Command::new("hg");
    init.current_dir(root)
        .args(["init", "repo"])
        .args(["--config", "format.use-share-safe=true"])
        .args(["--config", "format.generaldelta=true"])
        .args(["--config", "format.revlog-compression=zstd"])
        .args(["--config", "format.sparse-revlog=true"])
        .args(["--config", "format.use-persistent-nodemap=false"])
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
    fs::write(repo.join("a.txt"), b"beta\n").expect("rewrite a.txt");
    assert_hg(
        &hg(&repo, &["commit", "-m", "second", "-u", "test"]),
        "hg commit",
    );

    let requires_path = repo.join(".hg").join("requires");
    let requires = fs::read_to_string(&requires_path).unwrap_or_else(|err| {
        panic!("read {}: {err}", requires_path.display());
    });
    assert!(
        requires.lines().any(|line| line == "share-safe"),
        "requires missing share-safe:\n{requires}"
    );
    let store_path = repo.join(".hg").join("store").join("requires");
    let store_requires = fs::read_to_string(&store_path).unwrap_or_else(|err| {
        panic!("read {}: {err}", store_path.display());
    });
    for line in [
        "sparserevlog",
        "generaldelta",
        "revlogv1",
        "revlog-compression-zstd",
    ] {
        assert!(
            store_requires.lines().any(|got| got == line),
            "store/requires missing {line}:\n{store_requires}"
        );
    }
    repo
}

fn assert_cloned_tip(label: &str, clone: &Output, dest: &Path, tip_node: &str, log: &Path) {
    assert!(
        clone.status.success(),
        "{label} clone failed: status {:?}\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        clone.status,
        String::from_utf8_lossy(&clone.stdout),
        String::from_utf8_lossy(&clone.stderr),
        read_log(log)
    );
    let logged = hg(dest, &["log", "-r", "tip", "-T", "{node}"]);
    assert!(
        logged.status.success(),
        "{label} tip failed: status {:?}\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        logged.status,
        String::from_utf8_lossy(&logged.stdout),
        String::from_utf8_lossy(&logged.stderr),
        read_log(log)
    );
    let got = String::from_utf8_lossy(&logged.stdout);
    assert_eq!(
        got.trim(),
        tip_node,
        "{label} tip {got:?} != {tip_node}\nmononoke:\n{}",
        read_log(log)
    );
    let status = hg(dest, &["status"]);
    assert!(
        status.status.success(),
        "{label} status failed: status {:?}\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        status.status,
        String::from_utf8_lossy(&status.stdout),
        String::from_utf8_lossy(&status.stderr),
        read_log(log)
    );
    assert!(
        status.stdout.is_empty(),
        "{label} status stdout {:?}\nstderr:\n{}\nmononoke:\n{}",
        String::from_utf8_lossy(&status.stdout),
        String::from_utf8_lossy(&status.stderr),
        read_log(log)
    );
    let body = fs::read(dest.join("a.txt"))
        .unwrap_or_else(|err| panic!("{label} read a.txt: {err}\nmononoke:\n{}", read_log(log)));
    assert_eq!(
        body,
        b"beta\n",
        "{label} a.txt {body:?}\nmononoke:\n{}",
        read_log(log)
    );
}

/// Self-signed PEM pair for `https://127.0.0.1/`.
/// The IP is a subject alternative name. Mercurial ignores IP SANs and
/// matches the common name, so the CN is the same address.
fn write_test_cert(dir: &Path) -> (PathBuf, PathBuf) {
    let mut params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()])
        .expect("cert params for 127.0.0.1");
    let mut name = rcgen::DistinguishedName::new();
    name.push(rcgen::DnType::CommonName, "127.0.0.1");
    params.distinguished_name = name;
    let key_pair = rcgen::KeyPair::generate().expect("key pair");
    let cert = params.self_signed(&key_pair).expect("self-signed cert");
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    fs::write(&cert_path, cert.pem()).expect("write cert");
    fs::write(&key_path, key_pair.serialize_pem()).expect("write key");
    (cert_path, key_path)
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
    port: u16,
}

impl Running {
    fn start(repo: &Path, log: &Path) -> Self {
        let _guard = lock_ports();
        let port = free_port();
        Self::spawn(repo, log, port, &[])
    }

    fn start_https(repo: &Path, log: &Path, cert: &Path, key: &Path) -> (Self, u16) {
        let _guard = lock_ports();
        let http_port = free_port();
        let https_port = free_port_except(http_port);
        let extra = [
            "--https-port".to_string(),
            https_port.to_string(),
            "--tls-cert".to_string(),
            cert.display().to_string(),
            "--tls-key".to_string(),
            key.display().to_string(),
        ];
        (Self::spawn(repo, log, http_port, &extra), https_port)
    }

    fn spawn(repo: &Path, log: &Path, port: u16, extra: &[String]) -> Self {
        let bin = sibling_bin("mononoke");
        let log_file =
            File::create(log).unwrap_or_else(|err| panic!("log {}: {err}", log.display()));
        let stderr = log_file
            .try_clone()
            .unwrap_or_else(|err| panic!("clone log handle: {err}"));
        let mut child = Command::new(&bin)
            .arg("--http-port")
            .arg(port.to_string())
            .args(extra)
            .arg(repo)
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(stderr))
            .spawn()
            .unwrap_or_else(|err| panic!("spawn {}: {err}", bin.display()));
        let sock = repo.join(".hg").join("mononoke.sock");
        let started = Instant::now();
        loop {
            if sock.exists() {
                return Running { child, port };
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
fn http_clone_and_push() {
    let temp = TempDir::new("h");
    let repo = init_zstd_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let url = format!("http://127.0.0.1:{}/", server.port);
    let clone = hg(&temp.0, &["clone", &url, "dest"]);
    assert!(
        clone.status.success(),
        "hg clone failed: status {:?}\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        clone.status,
        String::from_utf8_lossy(&clone.stdout),
        String::from_utf8_lossy(&clone.stderr),
        read_log(&log)
    );
    let dest = temp.0.join("dest");
    let body = fs::read(dest.join("a.txt"))
        .unwrap_or_else(|err| panic!("read dest a.txt: {err}\nmononoke:\n{}", read_log(&log)));
    assert_eq!(
        body,
        b"alpha\n",
        "a.txt bytes {body:?}\nmononoke:\n{}",
        read_log(&log)
    );

    fs::write(dest.join("a.txt"), b"beta\n").expect("write beta");
    assert_hg(
        &hg(&dest, &["commit", "-m", "second", "-u", "test"]),
        "commit second",
    );
    let push = hg(&dest, &["push", &url]);
    assert!(
        push.status.success(),
        "hg push failed: status {:?}\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        push.status,
        String::from_utf8_lossy(&push.stdout),
        String::from_utf8_lossy(&push.stderr),
        read_log(&log)
    );

    let logged = hg(&repo, &["log", "-T", "{desc}"]);
    assert_hg(&logged, "served hg log");
    let text = String::from_utf8_lossy(&logged.stdout);
    assert!(
        text.contains("second"),
        "log {text:?} does not contain second\nmononoke:\n{}",
        read_log(&log)
    );

    assert_hg(&hg(&repo, &["update", "-C"]), "update served repo");
    let served = fs::read(repo.join("a.txt")).expect("served a.txt");
    assert_eq!(
        served,
        b"beta\n",
        "served a.txt {served:?}\nmononoke:\n{}",
        read_log(&log)
    );
}

#[test]
fn https_clone_and_push() {
    let temp = TempDir::new("hs");
    let repo = init_zstd_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let (cert, key) = write_test_cert(&temp.0);
    let (server, https_port) = Running::start_https(&repo, &log, &cert, &key);
    let url = format!("https://127.0.0.1:{https_port}/");
    let cacerts = format!("web.cacerts={}", cfg_path(&cert));
    let clone = hg(&temp.0, &["clone", &url, "dest", "--config", &cacerts]);
    assert!(
        clone.status.success(),
        "hg clone failed: status {:?}\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        clone.status,
        String::from_utf8_lossy(&clone.stdout),
        String::from_utf8_lossy(&clone.stderr),
        read_log(&log)
    );
    let dest = temp.0.join("dest");
    let body = fs::read(dest.join("a.txt"))
        .unwrap_or_else(|err| panic!("read dest a.txt: {err}\nmononoke:\n{}", read_log(&log)));
    assert_eq!(
        body,
        b"alpha\n",
        "a.txt bytes {body:?}\nmononoke:\n{}",
        read_log(&log)
    );

    fs::write(dest.join("a.txt"), b"beta\n").expect("write beta");
    assert_hg(
        &hg(&dest, &["commit", "-m", "second", "-u", "test"]),
        "commit second",
    );
    let push = hg(&dest, &["push", &url, "--config", &cacerts]);
    assert!(
        push.status.success(),
        "hg push failed: status {:?}\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        push.status,
        String::from_utf8_lossy(&push.stdout),
        String::from_utf8_lossy(&push.stderr),
        read_log(&log)
    );

    let logged = hg(&repo, &["log", "-T", "{desc}"]);
    assert_hg(&logged, "served hg log");
    let text = String::from_utf8_lossy(&logged.stdout);
    assert!(
        text.contains("second"),
        "log {text:?} does not contain second\nmononoke:\n{}",
        read_log(&log)
    );

    assert_hg(&hg(&repo, &["update", "-C"]), "update served repo");
    let served = fs::read(repo.join("a.txt")).expect("served a.txt");
    assert_eq!(
        served,
        b"beta\n",
        "served a.txt {served:?}\nmononoke:\n{}",
        read_log(&log)
    );
    drop(server);
}

#[test]
fn https_clone_fails_without_the_server_cert() {
    let temp = TempDir::new("hf");
    let repo = init_zstd_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let (cert, key) = write_test_cert(&temp.0);
    let (_server, https_port) = Running::start_https(&repo, &log, &cert, &key);
    let url = format!("https://127.0.0.1:{https_port}/");
    let dest = temp.0.join("dest");
    let clone = hg(&temp.0, &["clone", &url, "dest"]);
    assert!(
        !clone.status.success(),
        "hg clone trusted an untrusted cert: status {:?}\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        clone.status,
        String::from_utf8_lossy(&clone.stdout),
        String::from_utf8_lossy(&clone.stderr),
        read_log(&log)
    );
    assert!(
        !dest.join("a.txt").exists(),
        "working copy created without trusting the server cert\nmononoke:\n{}",
        read_log(&log)
    );
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

fn changelog_len(repo: &Path) -> u64 {
    let index = repo.join(".hg").join("store").join("00changelog.i");
    fs::metadata(&index)
        .unwrap_or_else(|err| panic!("changelog {}: {err}", index.display()))
        .len()
}

#[test]
fn ssh_push_appears_in_served_repo() {
    let temp = TempDir::new("u");
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
    fs::write(dest.join("a.txt"), b"beta\n").expect("write beta");
    assert_hg(
        &hg(&dest, &["commit", "-m", "second", "-u", "test"]),
        "commit second",
    );
    let push = hg_ssh(&dest, &["push"], &shim);
    assert!(
        push.status.success(),
        "hg push failed: status {:?}\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        push.status,
        String::from_utf8_lossy(&push.stdout),
        String::from_utf8_lossy(&push.stderr),
        read_log(&log)
    );

    let logged = hg(&repo, &["log", "-T", "{desc}"]);
    assert_hg(&logged, "served hg log");
    let text = String::from_utf8_lossy(&logged.stdout);
    assert!(
        text.contains("second"),
        "log {text:?} does not contain second\nmononoke:\n{}",
        read_log(&log)
    );

    assert_hg(&hg(&repo, &["update", "-C"]), "update served repo");
    let served = fs::read(repo.join("a.txt")).expect("served a.txt");
    assert_eq!(served, b"beta\n", "served a.txt {served:?}");

    let again = hg_ssh(&temp.0, &["clone", "ssh://user@dummy/repo", "dest2"], &shim);
    assert!(
        again.status.success(),
        "second clone failed: status {:?}\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        again.status,
        String::from_utf8_lossy(&again.stdout),
        String::from_utf8_lossy(&again.stderr),
        read_log(&log)
    );
    let cloned = fs::read(temp.0.join("dest2").join("a.txt")).expect("cloned a.txt");
    assert_eq!(cloned, b"beta\n", "cloned a.txt {cloned:?}");
}

#[test]
fn bundle1_push_over_ssh_and_http() {
    for ssh in [true, false] {
        let temp = TempDir::new("b1");
        let repo = init_zstd_repo(&temp.0);
        let log = temp.0.join("mononoke.log");
        let server = Running::start(&repo, &log);
        let shim = write_ssh_shim(&temp.0, &sibling_bin("hgcli"), &repo);
        let url = if ssh {
            "ssh://user@dummy/repo".to_string()
        } else {
            format!("http://127.0.0.1:{}/", server.port)
        };
        let run = |dir: &Path, args: &[&str]| {
            if ssh {
                hg_ssh(dir, args, &shim)
            } else {
                hg(dir, args)
            }
        };
        assert_ok_with_log(
            &run(&temp.0, &["clone", "--pull", &url, "dest"]),
            "clone",
            &log,
        );
        let dest = temp.0.join("dest");
        fs::write(dest.join("a.txt"), b"bundle1 edit\n").unwrap();
        assert_hg(
            &hg(&dest, &["commit", "-m", "bundle1", "-u", "test"]),
            "commit",
        );
        let push = run(
            &dest,
            &["push", "--config", "devel.legacy.exchange=bundle1"],
        );
        assert_ok_with_log(&push, "bundle1 push", &log);
        assert_same_tip(&repo, &dest);
        assert_eq!(
            hg(&repo, &["log", "-r", "tip", "-T", "{desc}"]).stdout,
            b"bundle1"
        );
    }
}

#[test]
fn bundle1_heads_race_matches_hg_serve() {
    use std::io::{BufRead, BufReader, Read, Write};
    let temp = TempDir::new("b1race");
    let repo = init_zstd_repo(&temp.0);
    let before = changelog_len(&repo);
    let log = temp.0.join("mononoke.log");
    let mono = Running::start(&repo, &log);
    let stock = HgServe::start(&repo);
    let query = format!("cmd=unbundle&heads={}", "0".repeat(40));
    let expected = raw_command(stock.port, &query);
    assert!(expected.starts_with(b"0\n") && expected.len() > 3);
    assert_eq!(raw_command(mono.port, &query), expected);
    let mut replies = Vec::new();
    for binary in [PathBuf::from("hg"), sibling_bin("hgcli")] {
        let mut child = Command::new(binary)
            .args(["-R", &cfg_path(&repo), "serve", "--stdio"])
            .env("HGPLAIN", "1")
            .env("HGRCPATH", "")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        write!(input, "unbundle\nheads 40\n{}", "0".repeat(40)).unwrap();
        input.flush().unwrap();
        // Keep stdin open until the reply arrives: hgcli exits on EOF.
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let mut length = String::new();
        output.read_line(&mut length).unwrap();
        let mut body = vec![0; length.trim().parse::<usize>().unwrap()];
        output.read_exact(&mut body).unwrap();
        let mut reply = length.into_bytes();
        reply.extend(body);
        drop(input);
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        replies.push(reply);
    }
    assert_eq!(replies[1], replies[0]);
    assert_eq!(changelog_len(&repo), before);
}

#[test]
fn ssh_push_rejects_changed_remote_heads() {
    let temp = TempDir::new("r");
    let repo = init_zstd_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let _server = Running::start(&repo, &log);
    let shim = write_ssh_shim(&temp.0, &sibling_bin("hgcli"), &repo);

    let first = hg_ssh(&temp.0, &["clone", "ssh://user@dummy/repo", "first"], &shim);
    assert!(
        first.status.success(),
        "first clone failed: status {:?}\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        first.status,
        String::from_utf8_lossy(&first.stdout),
        String::from_utf8_lossy(&first.stderr),
        read_log(&log)
    );
    let second = hg_ssh(
        &temp.0,
        &["clone", "ssh://user@dummy/repo", "second"],
        &shim,
    );
    assert!(
        second.status.success(),
        "second clone failed: status {:?}\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        second.status,
        String::from_utf8_lossy(&second.stdout),
        String::from_utf8_lossy(&second.stderr),
        read_log(&log)
    );

    let first_dir = temp.0.join("first");
    fs::write(first_dir.join("first.txt"), b"first\n").expect("write first.txt");
    assert_hg(&hg(&first_dir, &["add", "first.txt"]), "add first.txt");
    assert_hg(
        &hg(&first_dir, &["commit", "-m", "from first", "-u", "test"]),
        "commit first",
    );

    let second_dir = temp.0.join("second");
    fs::write(second_dir.join("second.txt"), b"second\n").expect("write second.txt");
    assert_hg(&hg(&second_dir, &["add", "second.txt"]), "add second.txt");
    assert_hg(
        &hg(&second_dir, &["commit", "-m", "from second", "-u", "test"]),
        "commit second",
    );
    // Discovery freezes the heads the client saw. preoutgoing then lands the
    // other clone, so check:heads is stale by the time the bundle is applied.
    // A push after that discovery would die locally with "new remote head"
    // and never send the part.
    let hook_path = temp.0.join("push-first.cmd");
    let hook = format!(
        "@echo off\r\nset HGPLAIN=1\r\nset HGRCPATH=\r\nhg -R \"{}\" --config ui.ssh=\"{}\" push\r\n",
        first_dir.display(),
        cfg_path(&shim)
    );
    fs::write(&hook_path, hook).expect("write hook");
    let hook_cfg = format!("hooks.preoutgoing={}", cfg_path(&hook_path));
    let raced = hg_ssh(
        &second_dir,
        &[
            "push",
            "--config",
            hook_cfg.as_str(),
            "--config",
            "hooks.preoutgoing:run-with-plain=true",
        ],
        &shim,
    );
    let raced_err = String::from_utf8_lossy(&raced.stderr);
    assert!(
        !raced.status.success(),
        "stale push succeeded: status {:?}\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        raced.status,
        String::from_utf8_lossy(&raced.stdout),
        raced_err,
        read_log(&log)
    );
    assert!(
        raced_err.contains("push failed:"),
        "stderr missing push failed:\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        String::from_utf8_lossy(&raced.stdout),
        raced_err,
        read_log(&log)
    );

    assert_hg(&hg(&repo, &["update", "-C"]), "update served repo");
    let served_first = fs::read(repo.join("first.txt"))
        .unwrap_or_else(|err| panic!("served first.txt: {err}\nmononoke:\n{}", read_log(&log)));
    assert_eq!(
        served_first, b"first\n",
        "served first.txt {served_first:?}"
    );
    assert!(
        !repo.join("second.txt").exists(),
        "served repo contains second.txt\nmononoke:\n{}",
        read_log(&log)
    );

    let later = hg_ssh(&temp.0, &["clone", "ssh://user@dummy/repo", "later"], &shim);
    assert!(
        later.status.success(),
        "later clone failed: status {:?}\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        later.status,
        String::from_utf8_lossy(&later.stdout),
        String::from_utf8_lossy(&later.stderr),
        read_log(&log)
    );
    let later_dir = temp.0.join("later");
    let cloned_first = fs::read(later_dir.join("first.txt")).expect("cloned first.txt");
    assert_eq!(
        cloned_first, b"first\n",
        "cloned first.txt {cloned_first:?}"
    );
    assert!(
        !later_dir.join("second.txt").exists(),
        "later clone contains second.txt"
    );
}

#[test]
fn ssh_push_bookmark_updates_served_repo() {
    let temp = TempDir::new("k");
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
    fs::write(dest.join("a.txt"), b"beta\n").expect("write beta");
    assert_hg(
        &hg(&dest, &["commit", "-m", "second", "-u", "test"]),
        "commit second",
    );
    assert_hg(&hg(&dest, &["bookmark", "pushed"]), "hg bookmark");
    let tip = hg(&dest, &["log", "-r", ".", "-T", "{node}"]);
    assert_hg(&tip, "tip node");
    let tip_node = String::from_utf8_lossy(&tip.stdout).trim().to_owned();

    let push = hg_ssh(&dest, &["push", "-B", "pushed"], &shim);
    assert!(
        push.status.success(),
        "hg push bookmark failed: status {:?}\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        push.status,
        String::from_utf8_lossy(&push.stdout),
        String::from_utf8_lossy(&push.stderr),
        read_log(&log)
    );

    let marks_path = repo.join(".hg").join("bookmarks");
    let marks = fs::read(&marks_path).unwrap_or_default();
    let text = String::from_utf8_lossy(&marks);
    let expected = format!("{tip_node} pushed\n");
    assert!(
        text.contains(&expected),
        "bookmarks {text:?} missing {expected:?}\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        String::from_utf8_lossy(&push.stdout),
        String::from_utf8_lossy(&push.stderr),
        read_log(&log)
    );

    let zeros = "0000000000000000000000000000000000000000";
    let absent = hg_ssh(
        &dest,
        &[
            "debugpushkey",
            "ssh://user@dummy/repo",
            "bookmarks",
            "absent",
            zeros,
            tip_node.as_str(),
        ],
        &shim,
    );
    assert!(
        absent.status.success(),
        "forty-zero old failed: status {:?}\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        absent.status,
        String::from_utf8_lossy(&absent.stdout),
        String::from_utf8_lossy(&absent.stderr),
        read_log(&log)
    );
    let marks = fs::read(&marks_path).unwrap_or_default();
    let text = String::from_utf8_lossy(&marks);
    let absent_line = format!("{tip_node} absent\n");
    assert!(
        text.contains(&expected) && text.contains(&absent_line),
        "bookmarks {text:?} missing pushed or absent"
    );

    let mismatch = hg_ssh(
        &dest,
        &[
            "debugpushkey",
            "ssh://user@dummy/repo",
            "bookmarks",
            "pushed",
            "1111111111111111111111111111111111111111",
            "2222222222222222222222222222222222222222",
        ],
        &shim,
    );
    let after = fs::read(&marks_path).unwrap_or_default();
    assert_eq!(
        after,
        marks,
        "mismatched old changed bookmarks to {}\nstdout:\n{}\nstderr:\n{}\nmononoke:\n{}",
        String::from_utf8_lossy(&after),
        String::from_utf8_lossy(&mismatch.stdout),
        String::from_utf8_lossy(&mismatch.stderr),
        read_log(&log)
    );
}

#[test]
fn unbundle_rejects_mismatched_heads_without_writing() {
    let temp = TempDir::new("m");
    let repo = init_zstd_repo(&temp.0);
    let before = changelog_len(&repo);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let mut raw = [0u8; 20];
    raw[..8].copy_from_slice(&(nanos as u64).to_le_bytes());
    raw[0] |= 0x01;
    let head = mercurial_types::NodeHash::from_bytes(&raw).expect("fresh node");
    let reply =
        mononoke::push::apply_unbundle(&repo.join(".hg"), &[head], b"").expect("apply_unbundle");
    assert_eq!(reply, b"0\n");
    assert_eq!(changelog_len(&repo), before, "changelog length changed");
}

#[test]
fn share_safe_sparse_repo_clones_over_ssh_http_and_https() {
    let temp = TempDir::new("ss");
    let repo = init_share_safe_repo(&temp.0);
    let tip = hg(&repo, &["log", "-r", "tip", "-T", "{node}"]);
    assert_hg(&tip, "source tip");
    let tip_node = String::from_utf8_lossy(&tip.stdout).trim().to_owned();

    let log = temp.0.join("mononoke.log");
    let (cert, key) = write_test_cert(&temp.0);
    let (server, https_port) = Running::start_https(&repo, &log, &cert, &key);
    let http_url = format!("http://127.0.0.1:{}/", server.port);
    let https_url = format!("https://127.0.0.1:{https_port}/");
    let cacerts = format!("web.cacerts={}", cfg_path(&cert));
    let shim = write_ssh_shim(&temp.0, &sibling_bin("hgcli"), &repo);

    let ssh = hg_ssh(
        &temp.0,
        &["clone", "ssh://user@dummy/repo", "ssh-dest"],
        &shim,
    );
    assert_cloned_tip("ssh", &ssh, &temp.0.join("ssh-dest"), &tip_node, &log);

    let http = hg(&temp.0, &["clone", &http_url, "http-dest"]);
    assert_cloned_tip("http", &http, &temp.0.join("http-dest"), &tip_node, &log);

    let https = hg(
        &temp.0,
        &["clone", &https_url, "https-dest", "--config", &cacerts],
    );
    assert_cloned_tip("https", &https, &temp.0.join("https-dest"), &tip_node, &log);
    drop(server);
}

#[test]
fn stream_clone_and_changegroup_clone_match() {
    let temp = TempDir::new("st");
    let repo = init_share_safe_repo(&temp.0);
    // Long enough that its filelog is hashed under `store/dh`.
    let long = format!("Long{}.txt", "x".repeat(130));
    fs::write(repo.join(&long), b"long\n").expect("write long file");
    // A directory Mercurial's fncache stores as `changelog.d.hg/`.
    fs::create_dir_all(repo.join("changelog.d")).expect("mkdir changelog.d");
    fs::write(repo.join("changelog.d").join("note.txt"), b"note\n").expect("write note");
    assert_hg(
        &hg(&repo, &["add", &long, "changelog.d/note.txt"]),
        "hg add long",
    );
    assert_hg(
        &hg(&repo, &["commit", "-m", "long", "-u", "test"]),
        "hg commit long",
    );
    assert!(
        repo.join(".hg").join("store").join("dh").exists(),
        "no store/dh"
    );
    let tip = hg(&repo, &["log", "-r", "tip", "-T", "{node}"]);
    assert_hg(&tip, "source tip");
    let tip_node = String::from_utf8_lossy(&tip.stdout).trim().to_owned();

    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let url = format!("http://127.0.0.1:{}/", server.port);
    let shim = write_ssh_shim(&temp.0, &sibling_bin("hgcli"), &repo);

    let clones = [
        (
            "ssh stream",
            hg_ssh(
                &temp.0,
                &["clone", "ssh://user@dummy/repo", "ssh-stream"],
                &shim,
            ),
            "ssh-stream",
            "streaming all changes",
        ),
        (
            "http stream",
            hg(&temp.0, &["clone", &url, "http-stream"]),
            "http-stream",
            "streaming all changes",
        ),
        (
            "http changegroup",
            hg(&temp.0, &["clone", "--pull", &url, "http-cg"]),
            "http-cg",
            "adding changesets",
        ),
    ];
    for (label, clone, dir, expect) in clones {
        let dest = temp.0.join(dir);
        assert_cloned_tip(label, &clone, &dest, &tip_node, &log);
        let stdout = String::from_utf8_lossy(&clone.stdout);
        assert!(
            stdout.contains(expect),
            "{label} stdout missing {expect:?}:\n{stdout}"
        );
        assert_hg(&hg(&dest, &["verify"]), &format!("{label} verify"));
        let body = fs::read(dest.join(&long)).expect("long file");
        assert_eq!(body, b"long\n", "{label} long file");
        let note = fs::read(dest.join("changelog.d").join("note.txt")).expect("note");
        assert_eq!(note, b"note\n", "{label} changelog.d/note.txt");
    }
    // Over HTTP the stream clone goes out uncompressed and the changegroup as zstd.
    let served = read_log(&log);
    assert!(served.contains("bytes streamed (none)"), "{served}");
    assert!(served.contains("bytes streamed (zstd)"), "{served}");
    drop(server);
}

#[test]
fn ssh_push_records_mercurial_fncache_names() {
    let temp = TempDir::new("fn");
    let repo = init_zstd_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let shim = write_ssh_shim(&temp.0, &sibling_bin("hgcli"), &repo);
    let clone = hg_ssh(&temp.0, &["clone", "ssh://user@dummy/repo", "dest"], &shim);
    assert_hg(&clone, "clone");

    let dest = temp.0.join("dest");
    let long = format!("Long{}.txt", "x".repeat(130));
    fs::write(dest.join("Mixed.txt"), b"mixed\n").expect("write Mixed.txt");
    fs::create_dir_all(dest.join("changelog.d")).expect("mkdir changelog.d");
    fs::write(dest.join("changelog.d").join("note.txt"), b"note\n").expect("write note");
    fs::write(dest.join(&long), b"long\n").expect("write long");
    // Incompressible and past the inline limit, so the filelog is split into a `.d`.
    let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
    let big: Vec<u8> = (0..200_000)
        .map(|_| {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            (seed >> 56) as u8
        })
        .collect();
    fs::write(dest.join("big.bin"), &big).expect("write big.bin");
    assert_hg(
        &hg(
            &dest,
            &["add", "Mixed.txt", "changelog.d/note.txt", &long, "big.bin"],
        ),
        "add",
    );
    assert_hg(
        &hg(&dest, &["commit", "-m", "names", "-u", "test"]),
        "commit",
    );
    let push = hg_ssh(&dest, &["push"], &shim);
    assert_hg(&push, "push");

    let fncache =
        fs::read_to_string(repo.join(".hg").join("store").join("fncache")).expect("served fncache");
    let lines: Vec<&str> = fncache.lines().collect();
    let long_entry = format!("data/{long}.i");
    for want in [
        "data/a.txt.i",
        "data/Mixed.txt.i",
        "data/changelog.d.hg/note.txt.i",
        long_entry.as_str(),
        "data/big.bin.i",
        "data/big.bin.d",
    ] {
        assert!(
            lines.contains(&want),
            "fncache missing {want:?}:\n{fncache}"
        );
    }
    assert_eq!(lines.len(), 6, "fncache:\n{fncache}");

    let verify = hg(&repo, &["verify"]);
    assert_hg(&verify, "served verify");
    let stderr = String::from_utf8_lossy(&verify.stderr);
    assert!(
        !stderr.contains("warning"),
        "served verify warned:\n{stderr}"
    );

    // A stream clone lists the store through fncache.
    let url = format!("http://127.0.0.1:{}/", server.port);
    let streamed = hg(&temp.0, &["clone", &url, "streamed"]);
    assert_hg(&streamed, "stream clone");
    let streamed_dir = temp.0.join("streamed");
    assert_hg(&hg(&streamed_dir, &["verify"]), "stream clone verify");
    assert_eq!(
        fs::read(streamed_dir.join("big.bin")).expect("streamed big.bin"),
        big
    );
    drop(server);
}

#[test]
fn stream_clone_after_push_sees_the_push() {
    let temp = TempDir::new("sp");
    let repo = init_zstd_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let url = format!("http://127.0.0.1:{}/", server.port);
    let shim = write_ssh_shim(&temp.0, &sibling_bin("hgcli"), &repo);

    // The first stream clone lists the store, and the server keeps that listing.
    let first = hg(&temp.0, &["clone", &url, "first"]);
    assert_hg(&first, "first stream clone");
    let first_dir = temp.0.join("first");
    fs::write(first_dir.join("pushed.txt"), b"pushed\n").expect("write pushed.txt");
    assert_hg(&hg(&first_dir, &["add", "pushed.txt"]), "add pushed.txt");
    assert_hg(
        &hg(&first_dir, &["commit", "-m", "pushed", "-u", "test"]),
        "commit pushed",
    );
    assert_hg(&hg_ssh(&first_dir, &["push"], &shim), "push");

    let second = hg(&temp.0, &["clone", &url, "second"]);
    assert_hg(&second, "second stream clone");
    let stdout = String::from_utf8_lossy(&second.stdout);
    assert!(
        stdout.contains("streaming all changes"),
        "not streamed:\n{stdout}"
    );
    let second_dir = temp.0.join("second");
    assert_hg(&hg(&second_dir, &["verify"]), "second clone verify");
    assert_eq!(
        fs::read(second_dir.join("pushed.txt")).expect("pushed.txt in second clone"),
        b"pushed\n"
    );
    drop(server);
}

#[test]
fn dirstate_v2_tracked_key_repo_is_served_and_clients_keep_their_own_dirstate() {
    // Without Mercurial's Rust extensions, hg refuses dirstate-v2 unless told the slow path is fine.
    const SLOW_DSV2: &str = "storage.dirstate-v2.slow-path=allow";
    let temp = TempDir::new("d2");
    let mut init = Command::new("hg");
    init.current_dir(&temp.0)
        .args(["init", "repo"])
        .args(["--config", "format.use-dirstate-v2=true"])
        .args(["--config", "format.use-dirstate-tracked-hint=true"])
        .args(["--config", SLOW_DSV2])
        .args(["--config", "format.use-persistent-nodemap=false"])
        .env("HGPLAIN", "1")
        .env("HGRCPATH", "");
    assert_hg(
        &output_timeout(&mut init, Duration::from_secs(120)),
        "hg init",
    );
    let repo = temp.0.join("repo");
    let requires = fs::read_to_string(repo.join(".hg").join("requires")).expect("requires");
    for line in ["dirstate-v2", "dirstate-tracked-key-v1"] {
        assert!(
            requires.lines().any(|got| got == line),
            "requires missing {line}:\n{requires}"
        );
    }
    fs::write(repo.join("a.txt"), b"alpha\n").expect("write a.txt");
    assert_hg(
        &hg(&repo, &["add", "a.txt", "--config", SLOW_DSV2]),
        "hg add",
    );
    assert_hg(
        &hg(
            &repo,
            &["commit", "-m", "first", "-u", "test", "--config", SLOW_DSV2],
        ),
        "hg commit",
    );

    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let url = format!("http://127.0.0.1:{}/", server.port);
    for (label, args) in [
        ("stream", vec!["clone", url.as_str(), "stream"]),
        (
            "changegroup",
            vec!["clone", "--pull", url.as_str(), "changegroup"],
        ),
    ] {
        let clone = hg(&temp.0, &args);
        assert_hg(&clone, &format!("{label} clone"));
        let dest = temp.0.join(label);
        assert_hg(&hg(&dest, &["verify"]), &format!("{label} verify"));
        assert_eq!(fs::read(dest.join("a.txt")).expect("a.txt"), b"alpha\n");
        // The working-copy format is the client's choice, not the server's.
        let cloned = fs::read_to_string(dest.join(".hg").join("requires")).expect("requires");
        for line in ["dirstate-v2", "dirstate-tracked-key-v1"] {
            assert!(
                !cloned.lines().any(|got| got == line),
                "{label} clone took {line}:\n{cloned}"
            );
        }
    }
    drop(server);
}

#[test]
fn lookup_resolves_names_and_clone_r_gets_only_ancestors() {
    // `--debug` surrounds the hash with protocol chatter, so pick the hash line.
    let last_line = |out: &Output| {
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(str::trim)
            .filter(|line| line.len() == 40 && line.bytes().all(|b| b.is_ascii_hexdigit()))
            .last()
            .unwrap_or("")
            .to_owned()
    };
    let temp = TempDir::new("lk");
    let repo = init_zstd_repo(&temp.0);
    for (rev, text) in [(1, "beta\n"), (2, "gamma\n")] {
        fs::write(repo.join("a.txt"), text).expect("write a.txt");
        assert_hg(
            &hg(&repo, &["commit", "-m", &format!("rev{rev}"), "-u", "test"]),
            "hg commit",
        );
    }
    assert_hg(
        &hg(&repo, &["bookmark", "-r", "1", "feature"]),
        "hg bookmark",
    );
    let node = |rev: &str| {
        let out = hg(&repo, &["log", "-r", rev, "-T", "{node}"]);
        assert_hg(&out, "hg log");
        String::from_utf8_lossy(&out.stdout).trim().to_owned()
    };
    let (n0, n1, n2) = (node("0"), node("1"), node("2"));

    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let url = format!("http://127.0.0.1:{}/", server.port);
    let shim = write_ssh_shim(&temp.0, &sibling_bin("hgcli"), &repo);
    // `hg identify -r KEY URL` resolves KEY with the remote `lookup` command.
    let identify = |key: &str| hg(&temp.0, &["identify", "--debug", "-i", "-r", key, &url]);
    let null = "0".repeat(40);
    for (key, want) in [
        ("tip", &n2),
        ("0", &n0),
        ("-1", &n2),
        (n1.as_str(), &n1),
        (&n1[..8], &n1),
        ("feature", &n1),
        ("null", &null),
    ] {
        let out = identify(key);
        assert_hg(&out, &format!("identify {key}"));

        let got = last_line(&out);
        assert_eq!(&got, want, "lookup {key}\nmononoke:\n{}", read_log(&log));
    }
    let missing = identify("nope");
    assert!(!missing.status.success(), "lookup of nope succeeded");
    let stderr = String::from_utf8_lossy(&missing.stderr);
    assert!(stderr.contains("unknown revision 'nope'"), "{stderr}");

    let ssh = hg_ssh(
        &temp.0,
        &[
            "identify",
            "--debug",
            "-i",
            "-r",
            "feature",
            "ssh://user@dummy/repo",
        ],
        &shim,
    );
    assert_hg(&ssh, "ssh identify");
    assert_eq!(last_line(&ssh), n1);

    // `-r` sends getbundle `heads`, so the clone stops at rev 1.
    let partial = hg(&temp.0, &["clone", "-r", "feature", &url, "partial"]);
    assert_hg(&partial, "clone -r");
    let revs = hg(&temp.0.join("partial"), &["log", "-T", "{node}\n"]);
    assert_hg(&revs, "partial log");
    let mut got: Vec<String> = String::from_utf8_lossy(&revs.stdout)
        .lines()
        .map(str::to_owned)
        .collect();
    got.sort();
    let mut want = vec![n0.clone(), n1.clone()];
    want.sort();
    assert_eq!(got, want, "clone -r feature");
    drop(server);
}

#[test]
fn pushed_revision_is_stored_as_a_delta() {
    let temp = TempDir::new("pd");
    let repo = init_zstd_repo(&temp.0);
    // Incompressible, so a full-text snapshot would cost about its whole size.
    let mut seed: u64 = 0x5851_f42d_4c95_7f2d;
    let mut big: Vec<u8> = (0..60_000)
        .map(|_| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (seed >> 56) as u8
        })
        .collect();
    fs::write(repo.join("big.bin"), &big).expect("write big.bin");
    assert_hg(&hg(&repo, &["add", "big.bin"]), "hg add");
    assert_hg(
        &hg(&repo, &["commit", "-m", "big", "-u", "test"]),
        "hg commit",
    );

    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let shim = write_ssh_shim(&temp.0, &sibling_bin("hgcli"), &repo);
    let clone = hg_ssh(&temp.0, &["clone", "ssh://user@dummy/repo", "dest"], &shim);
    assert_hg(&clone, "clone");
    let dest = temp.0.join("dest");
    big[1000..1004].copy_from_slice(b"EDIT");
    fs::write(dest.join("big.bin"), &big).expect("edit big.bin");
    assert_hg(
        &hg(&dest, &["commit", "-m", "edit", "-u", "test"]),
        "commit edit",
    );
    assert_hg(&hg_ssh(&dest, &["push"], &shim), "push");
    drop(server);

    let served = mercurial::RevlogRepo::open(repo.join(".hg")).expect("open served repo");
    let filelog = served
        .get_file_revlog(&mercurial_types::Path::new(b"big.bin").expect("path"))
        .expect("big.bin filelog");
    let entry = filelog
        .get_entry(mercurial::revlog::RevIdx::from(1u32))
        .expect("pushed revision");
    assert_eq!(
        entry.baserev,
        Some(mercurial::revlog::RevIdx::from(0u32)),
        "pushed revision is not a delta against rev 0"
    );
    assert!(
        entry.compressed_len < 1_000,
        "pushed revision takes {} bytes",
        entry.compressed_len
    );
    assert_hg(&hg(&repo, &["verify"]), "served verify");
    assert_hg(&hg(&repo, &["update", "-C"]), "update served repo");
    assert_eq!(fs::read(repo.join("big.bin")).expect("served big.bin"), big);
}

#[test]
fn push_waits_while_hg_holds_the_store_lock() {
    let temp = TempDir::new("lw");
    let repo = init_zstd_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let shim = write_ssh_shim(&temp.0, &sibling_bin("hgcli"), &repo);
    assert_hg(
        &hg_ssh(&temp.0, &["clone", "ssh://user@dummy/repo", "dest"], &shim),
        "clone",
    );
    let dest = temp.0.join("dest");
    fs::write(dest.join("a.txt"), b"beta\n").expect("write beta");
    assert_hg(
        &hg(&dest, &["commit", "-m", "second", "-u", "test"]),
        "commit",
    );

    // Hold the store lock the way `hg` does while it writes.
    let lock = repo.join(".hg").join("store").join("lock");
    fs::write(&lock, b"otherhost:12345").expect("write lock");
    let before = changelog_len(&repo);
    let push = {
        let (dest, shim) = (dest.clone(), shim.clone());
        thread::spawn(move || hg_ssh(&dest, &["push"], &shim))
    };
    thread::sleep(Duration::from_secs(3));
    assert!(!push.is_finished(), "push finished while the lock was held");
    assert_eq!(
        changelog_len(&repo),
        before,
        "push wrote while the lock was held"
    );

    fs::remove_file(&lock).expect("release lock");
    assert_hg(&push.join().expect("push thread"), "push");
    assert!(changelog_len(&repo) > before, "push did not land");
    assert!(!lock.exists(), "server left the store lock behind");
    assert!(
        !repo.join(".hg").join("wlock").exists(),
        "server left the wlock behind"
    );
    drop(server);
}

#[test]
fn lfs_repo_clones_pointers_over_cg3_and_refuses_clients_without_it() {
    let temp = TempDir::new("lfsc");
    assert_hg(
        &hg(&temp.0, &["init", "repo", "--config", "extensions.lfs="]),
        "hg init",
    );
    let repo = temp.0.join("repo");
    let lfs = ["--config", "extensions.lfs=", "--config", "lfs.track=all()"];
    let hg_lfs = |dir: &Path, args: &[&str]| hg(dir, &[&lfs[..], args].concat());
    // Two revisions of the file, a plain one between them, and a sibling so
    // that `clone -r` takes the restricted path.
    for (text, track) in [("one\n", true), ("two\n", false), ("three\n", true)] {
        fs::write(repo.join("big.bin"), text).expect("write big.bin");
        let args = ["commit", "-Aqm", text, "-u", "test"];
        assert_hg(&if track { hg_lfs(&repo, &args) } else { hg(&repo, &args) }, "hg commit");
    }
    assert_hg(&hg(&repo, &["update", "-r", "0"]), "hg update");
    fs::write(repo.join("big.bin"), "sibling\n").expect("write big.bin");
    assert_hg(&hg_lfs(&repo, &["commit", "-Aqm", "sibling", "-u", "test"]), "hg commit");
    let raw = |dir: &Path, rev: &str| {
        let out = hg_lfs(dir, &["debugdata", "big.bin", rev]);
        assert_hg(&out, "hg debugdata");
        out.stdout
    };
    assert!(raw(&repo, "0").starts_with(b"version https://git-lfs"), "not a pointer");

    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let url = format!("http://127.0.0.1:{}/", server.port);

    assert_hg(
        &hg_lfs(&temp.0, &["clone", "-U", "--pull", &url, "whole"]),
        "lfs clone",
    );
    let whole = temp.0.join("whole");
    for rev in ["0", "1", "2", "3"] {
        assert_eq!(raw(&whole, rev), raw(&repo, rev), "rev {rev}");
    }
    // `-r` on rev 2 skips the sibling, whose filelog revision is stored last.
    assert_hg(
        &hg_lfs(&temp.0, &["clone", "-U", "-r", "2", &url, "partial"]),
        "lfs clone -r",
    );
    let partial = temp.0.join("partial");
    for rev in ["0", "1", "2"] {
        assert_eq!(raw(&partial, rev), raw(&repo, rev), "partial rev {rev}");
    }
    let flags = |dir: &Path| {
        let out = hg_lfs(dir, &["debugrevlogindex", "-f", "1", "big.bin"]);
        assert_hg(&out, "hg debugrevlogindex");
        String::from_utf8_lossy(&out.stdout).lines().skip(1)
            .map(|line| line.split_whitespace().nth(1).expect("flag column").to_owned())
            .collect::<Vec<_>>()
    };
    assert_eq!(flags(&whole), flags(&repo), "flags in the clone");
    assert_eq!(flags(&repo).iter().filter(|flag| *flag == "2000").count(), 3);

    // A pull, because `hg clone` sees `lfs-serve` and turns lfs on, and with it 03.
    assert_hg(&hg(&temp.0, &["init", "old"]), "hg init old");
    let refused = hg(
        &temp.0.join("old"),
        &["--config", "experimental.changegroup3=false", "pull", &url],
    );
    assert!(!refused.status.success(), "changegroup 02 pull of an lfs repo");
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(stderr.contains("this repository uses LFS"), "{stderr}");
}

#[test]
fn commit_made_with_hg_while_serving_is_served() {
    let temp = TempDir::new("oc");
    let repo = init_zstd_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let url = format!("http://127.0.0.1:{}/", server.port);
    // The first clones warm the server's revlogs and stream listing.
    assert_hg(
        &hg(&temp.0, &["clone", &url, "before"]),
        "stream clone before",
    );
    assert_hg(
        &hg(&temp.0, &["clone", "--pull", &url, "before-pull"]),
        "pull clone before",
    );

    // A commit made by hg itself, not pushed through the server.
    fs::write(repo.join("outside.txt"), b"outside\n").expect("write outside.txt");
    assert_hg(&hg(&repo, &["add", "outside.txt"]), "hg add");
    assert_hg(
        &hg(&repo, &["commit", "-m", "outside", "-u", "test"]),
        "hg commit",
    );
    let tip = hg(&repo, &["log", "-r", "tip", "-T", "{node}"]);
    assert_hg(&tip, "source tip");
    let tip_node = String::from_utf8_lossy(&tip.stdout).trim().to_owned();

    for (label, args) in [
        ("stream", vec!["clone", url.as_str(), "after"]),
        (
            "changegroup",
            vec!["clone", "--pull", url.as_str(), "after-pull"],
        ),
    ] {
        let clone = hg(&temp.0, &args);
        let dest = temp.0.join(args[args.len() - 1]);
        assert_hg(&clone, &format!("{label} clone after"));
        let got = hg(&dest, &["log", "-r", "tip", "-T", "{node}"]);
        assert_hg(&got, "clone tip");
        assert_eq!(
            String::from_utf8_lossy(&got.stdout).trim(),
            tip_node,
            "{label} clone missed the outside commit\nmononoke:\n{}",
            read_log(&log)
        );
        assert_hg(&hg(&dest, &["verify"]), &format!("{label} verify"));
    }
    let pulled = hg(&temp.0.join("before-pull"), &["pull", &url]);
    assert_hg(&pulled, "pull into the earlier clone");
    let got = hg(
        &temp.0.join("before-pull"),
        &["log", "-r", "tip", "-T", "{node}"],
    );
    assert_eq!(String::from_utf8_lossy(&got.stdout).trim(), tip_node);
    drop(server);
}

/// Clients must ask for a nodemap too, or Mercurial drops the streamed one.
const SLOW_NODEMAP: [&str; 4] = [
    "--config",
    "storage.revlog.persistent-nodemap.slow-path=allow",
    "--config",
    "format.use-persistent-nodemap=true",
];

fn hg_nm(repo: &Path, args: &[&str]) -> Output {
    let all: Vec<&str> = args.iter().copied().chain(SLOW_NODEMAP).collect();
    hg(repo, &all)
}

fn hg_ssh_nm(dir: &Path, args: &[&str], shim: &Path) -> Output {
    let all: Vec<&str> = args.iter().copied().chain(SLOW_NODEMAP).collect();
    hg_ssh(dir, &all, shim)
}

/// Repo with Mercurial 7.0's default formats and a persistent nodemap, a
/// changelog of 3000 revisions (a nodemap is only written for a non-inline
/// revlog) and one real commit with a file.
fn init_nodemap_repo(root: &Path) -> PathBuf {
    let repo = root.join("repo");
    let mut init = Command::new("hg");
    init.current_dir(root)
        .args(["init", "repo"])
        .args(SLOW_NODEMAP)
        .env("HGPLAIN", "1")
        .env("HGRCPATH", "");
    assert_hg(
        &output_timeout(&mut init, Duration::from_secs(120)),
        "hg init",
    );
    assert_hg(&hg_nm(&repo, &["debugbuilddag", "+3000"]), "debugbuilddag");
    fs::write(repo.join("a.txt"), b"alpha\n").expect("write a.txt");
    assert_hg(&hg_nm(&repo, &["add", "a.txt"]), "hg add");
    assert_hg(
        &hg_nm(&repo, &["commit", "-m", "first", "-u", "test"]),
        "hg commit",
    );
    assert!(
        repo.join(".hg/store/00changelog.n").exists(),
        "hg wrote no changelog nodemap"
    );
    repo
}

/// The server never writes a nodemap, so it lags behind pushes until hg updates it.
fn assert_nodemap_repo_sound(repo: &Path, what: &str) {
    assert_hg(&hg_nm(repo, &["debugupdatecaches"]), what);
    assert_hg(&hg_nm(repo, &["verify"]), what);
    assert_hg(&hg_nm(repo, &["debugnodemap", "--check"]), what);
}

#[test]
fn nodemap_repo_clones_pulls_and_pushes() {
    let temp = TempDir::new("nm");
    let repo = init_nodemap_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let url = format!("http://127.0.0.1:{}/", server.port);
    let shim = write_ssh_shim(&temp.0, &sibling_bin("hgcli"), &repo);
    let ssh_url = "ssh://user@dummy/repo";

    let ssh_stream = hg_ssh_nm(&temp.0, &["clone", ssh_url, "ssh-stream"], &shim);
    assert_hg(&ssh_stream, "ssh stream clone");
    assert!(
        String::from_utf8_lossy(&ssh_stream.stdout).contains("streaming all changes"),
        "not streamed"
    );
    assert!(
        temp.0.join("ssh-stream/.hg/store/00changelog.n").exists(),
        "stream clone has no changelog nodemap"
    );
    assert_hg(&hg_nm(&temp.0.join("ssh-stream"), &["verify"]), "verify");
    assert_hg(
        &hg_ssh_nm(&temp.0, &["clone", "--pull", ssh_url, "ssh-pull"], &shim),
        "ssh pull clone",
    );
    assert_hg(
        &hg_nm(&temp.0, &["clone", &url, "http-stream"]),
        "http stream clone",
    );
    assert!(
        temp.0.join("http-stream/.hg/store/00changelog.n").exists(),
        "stream clone has no changelog nodemap"
    );
    assert_hg(
        &hg_nm(&temp.0, &["clone", "--pull", &url, "http-pull"]),
        "http pull clone",
    );

    for (label, dir, over_ssh) in [
        ("ssh", "ssh-stream", true),
        ("http", "http-pull", false),
    ] {
        let clone = temp.0.join(dir);
        assert_hg(&hg_nm(&clone, &["pull", "-u", &url]), "pull");
        fs::write(clone.join(format!("{label}.txt")), label).expect("write");
        assert_hg(&hg_nm(&clone, &["add", &format!("{label}.txt")]), "add");
        assert_hg(
            &hg_nm(&clone, &["commit", "-m", label, "-u", "test"]),
            "commit",
        );
        let push = if over_ssh {
            hg_ssh_nm(&clone, &["push"], &shim)
        } else {
            hg_nm(&clone, &["push", &url])
        };
        assert!(
            push.status.success(),
            "{label} push failed:\n{}\n{}\nmononoke:\n{}",
            String::from_utf8_lossy(&push.stdout),
            String::from_utf8_lossy(&push.stderr),
            read_log(&log)
        );
    }
    drop(server);
    assert_nodemap_repo_sound(&repo, "served repo after pushes");
}

#[test]
fn nodemap_repo_commit_while_serving_is_served() {
    let temp = TempDir::new("nc");
    let repo = init_nodemap_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let url = format!("http://127.0.0.1:{}/", server.port);
    assert_hg(&hg_nm(&temp.0, &["clone", &url, "before"]), "first clone");

    fs::write(repo.join("outside.txt"), b"outside\n").expect("write");
    assert_hg(&hg_nm(&repo, &["add", "outside.txt"]), "hg add");
    assert_hg(
        &hg_nm(&repo, &["commit", "-m", "outside", "-u", "test"]),
        "hg commit",
    );
    let tip = hg_nm(&repo, &["log", "-r", "tip", "-T", "{node}"]);
    assert_hg(&tip, "tip");
    let tip_node = String::from_utf8_lossy(&tip.stdout).trim().to_owned();

    assert_hg(&hg_nm(&temp.0.join("before"), &["pull", &url]), "pull");
    let got = hg_nm(&temp.0.join("before"), &["log", "-r", "tip", "-T", "{node}"]);
    assert_eq!(String::from_utf8_lossy(&got.stdout).trim(), tip_node);

    assert_hg(&hg_nm(&temp.0, &["clone", &url, "after"]), "second clone");
    let after = temp.0.join("after");
    assert_hg(&hg_nm(&after, &["verify"]), "verify second clone");
    let got = hg_nm(&after, &["log", "-r", "tip", "-T", "{node}"]);
    assert_eq!(
        String::from_utf8_lossy(&got.stdout).trim(),
        tip_node,
        "mononoke:\n{}",
        read_log(&log)
    );
    drop(server);
}

/// Treemanifest repo with files in nested directories over two commits.
fn init_tree_repo(root: &Path) -> PathBuf {
    let repo = root.join("repo");
    let mut init = Command::new("hg");
    init.current_dir(root)
        .args(["init", "repo"])
        .args(["--config", "experimental.treemanifest=1"])
        .args(["--config", "format.use-persistent-nodemap=false"])
        .env("HGPLAIN", "1")
        .env("HGRCPATH", "");
    assert_hg(
        &output_timeout(&mut init, Duration::from_secs(120)),
        "hg init",
    );
    fs::create_dir_all(repo.join("a").join("deep")).expect("mkdir a/deep");
    fs::create_dir_all(repo.join("B")).expect("mkdir B");
    for (rev, text) in [("first", "1\n"), ("second", "2\n")] {
        fs::write(repo.join("a").join("x"), text).expect("write a/x");
        fs::write(repo.join("a").join("deep").join("z"), text).expect("write a/deep/z");
        fs::write(repo.join("B").join("y"), text).expect("write B/y");
        fs::write(repo.join("top.txt"), text).expect("write top.txt");
        assert_hg(
            &hg(&repo, &["commit", "-Aqm", rev, "-u", "test"]),
            "hg commit",
        );
    }
    repo
}

fn requirements(repo: &Path) -> String {
    let hg_dir = repo.join(".hg");
    let mut all = fs::read_to_string(hg_dir.join("requires")).expect("requires");
    all.push_str(&fs::read_to_string(hg_dir.join("store").join("requires")).unwrap_or_default());
    all
}

#[test]
fn tree_repo_clones_over_every_transport() {
    let temp = TempDir::new("tm");
    let repo = init_tree_repo(&temp.0);
    let files = hg(&repo, &["files", "-r", "tip"]);
    assert_hg(&files, "server files");
    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let url = format!("http://127.0.0.1:{}/", server.port);
    let shim = write_ssh_shim(&temp.0, &sibling_bin("hgcli"), &repo);
    let ssh_url = "ssh://user@dummy/repo";

    let clones = [
        ("ssh stream", hg_ssh(&temp.0, &["clone", ssh_url, "ssh-stream"], &shim)),
        (
            "ssh pull",
            hg_ssh(&temp.0, &["clone", "--pull", ssh_url, "ssh-pull"], &shim),
        ),
        ("http stream", hg(&temp.0, &["clone", &url, "http-stream"])),
        (
            "http pull",
            hg(&temp.0, &["clone", "--pull", &url, "http-pull"]),
        ),
    ];
    for (label, clone) in clones {
        let dest = temp.0.join(label.replace(' ', "-"));
        assert!(
            clone.status.success(),
            "{label} clone failed:\n{}\n{}\nmononoke:\n{}",
            String::from_utf8_lossy(&clone.stdout),
            String::from_utf8_lossy(&clone.stderr),
            read_log(&log)
        );
        let expect = if label.ends_with("stream") {
            "streaming all changes"
        } else {
            "adding changesets"
        };
        let stdout = String::from_utf8_lossy(&clone.stdout);
        assert!(stdout.contains(expect), "{label}: {stdout}");
        let reqs = requirements(&dest);
        assert!(
            reqs.lines().any(|line| line == "treemanifest"),
            "{label} requires:\n{reqs}"
        );
        assert_hg(&hg(&dest, &["verify"]), &format!("{label} verify"));
        let got = hg(&dest, &["files", "-r", "tip"]);
        assert_hg(&got, &format!("{label} files"));
        assert_eq!(got.stdout, files.stdout, "{label} files");
    }
    drop(server);
}

#[test]
fn restricted_tree_reuse_clone_and_pull_matches_stock() {
    for mixed in [false, true] {
        let temp = TempDir::new(if mixed { "mixed-reuse" } else { "tree-reuse" });
        assert_hg(&hg(&temp.0, &["init", "repo", "--config", if mixed {
            "experimental.treemanifest=0"
        } else { "experimental.treemanifest=1" }]), "init");
        let repo = temp.0.join("repo");
        fs::create_dir_all(repo.join("d")).unwrap();
        fs::write(repo.join("d/a"), b"root\n").unwrap();
        assert_hg(&hg(&repo, &["commit", "-Aqm", "root", "-u", "test"]), "root");
        if mixed {
            let path = repo.join(".hg/store/requires");
            let mut reqs = fs::read(&path).unwrap();
            reqs.extend_from_slice(b"treemanifest\n");
            fs::write(path, reqs).unwrap();
        }
        for name in ["sibling-one", "sibling-two"] {
            assert_hg(&hg(&repo, &["update", "-C", "-r", "0"]), "update root");
            fs::write(repo.join("d/a"), b"reused\n").unwrap();
            assert_hg(&hg(&repo, &["commit", "-qm", name, "-u", "test"]), "sibling");
        }
        assert_hg(&hg(&repo, &["verify"]), "verify source");
        let log = temp.0.join("mono.log");
        let mono = Running::start(&repo, &log);
        let stock = HgServe::start(&repo);
        for (name, port) in [("stock", stock.port), ("mono", mono.port)] {
            let url = format!("http://127.0.0.1:{port}/");
            for incremental in [false, true] {
                let dest = format!("{name}-{incremental}");
                let clone = hg(&temp.0, &["clone", "--pull", "-r", if incremental { "0" } else { "2" }, &url, &dest]);
                assert_ok_with_log(&clone, "restricted clone", &log);
                let dest = temp.0.join(dest);
                if incremental {
                    assert_ok_with_log(&hg(&dest, &["pull", "-r", "2", &url]), "restricted pull", &log);
                }
                assert_hg(&hg(&dest, &["verify"]), "verify restricted destination");
                assert_eq!(node_of(&dest, "tip"), node_of(&repo, "2"));
                assert_eq!(hg(&dest, &["cat", "-r", "tip", "d/a"]).stdout, b"reused\n");
            }
        }
    }
}

#[test]
fn flat_repo_still_offers_02() {
    let temp = TempDir::new("f2");
    let repo = init_share_safe_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let url = format!("http://127.0.0.1:{}/", server.port);
    let clone = hg(
        &temp.0,
        &[
            "clone",
            "--pull",
            &url,
            "dest",
            "--config",
            "experimental.changegroup3=false",
        ],
    );
    assert!(
        clone.status.success(),
        "clone failed:\n{}\n{}\nmononoke:\n{}",
        String::from_utf8_lossy(&clone.stdout),
        String::from_utf8_lossy(&clone.stderr),
        read_log(&log)
    );
    assert_hg(&hg(&temp.0.join("dest"), &["verify"]), "verify");
    drop(server);
}

/// Stock `hg serve` on a free port, killed on drop. Push is allowed over plain HTTP.
struct HgServe {
    child: Child,
    port: u16,
}

impl HgServe {
    fn start(repo: &Path) -> Self {
        Self::start_with_config(repo, &[])
    }

    fn start_with_config(repo: &Path, config: &[&str]) -> Self {
        let _guard = lock_ports();
        let port = free_port();
        let mut child = Command::new("hg")
            .current_dir(repo)
            .args(["serve", "-a", "127.0.0.1", "-p", &port.to_string()])
            .args(["--config", "web.push_ssl=false"])
            .args(["--config", "web.allow-push=*"])
            .args(config.iter().flat_map(|value| ["--config", *value]))
            .env("HGPLAIN", "1")
            .env("HGRCPATH", "")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn hg serve");
        let started = Instant::now();
        while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
            if started.elapsed() > Duration::from_secs(30) {
                let _ = child.kill();
                panic!("hg serve did not listen on {port}");
            }
            if let Ok(Some(status)) = child.try_wait() {
                panic!("hg serve exited {status}");
            }
            thread::sleep(Duration::from_millis(50));
        }
        HgServe { child, port }
    }
}

impl Drop for HgServe {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn assert_ok_with_log(output: &Output, what: &str, log: &Path) {
    assert!(
        output.status.success(),
        "{what} failed:\n{}\n{}\nmononoke:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
        read_log(log)
    );
}

/// The served repo verifies and has the client's tip files and contents.
fn assert_same_tip(served: &Path, client: &Path) {
    assert_hg(&hg(served, &["verify"]), "served verify");
    let files = hg(client, &["files", "-r", "tip"]);
    assert_hg(&files, "client files");
    let got = hg(served, &["files", "-r", "tip"]);
    assert_hg(&got, "served files");
    assert_eq!(
        String::from_utf8_lossy(&got.stdout),
        String::from_utf8_lossy(&files.stdout)
    );
    for file in String::from_utf8_lossy(&files.stdout).lines() {
        let want = hg(client, &["cat", "-r", "tip", file]);
        assert_hg(&want, "client cat");
        let got = hg(served, &["cat", "-r", "tip", file]);
        assert_hg(&got, "served cat");
        assert_eq!(got.stdout, want.stdout, "{file}");
    }
}

/// Serves `init_tree_repo` over ssh, clones it to `dest`, lets `edit` change
/// the clone, then commits and pushes. Returns the server, its ssh shim and log.
fn push_tree_edit(temp: &TempDir, edit: impl FnOnce(&Path)) -> (Running, PathBuf, PathBuf) {
    let repo = init_tree_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let shim = write_ssh_shim(&temp.0, &sibling_bin("hgcli"), &repo);
    let clone = hg_ssh(&temp.0, &["clone", "ssh://user@dummy/repo", "dest"], &shim);
    assert_ok_with_log(&clone, "clone", &log);
    let dest = temp.0.join("dest");
    edit(&dest);
    assert_hg(
        &hg(&dest, &["commit", "-Aqm", "edit", "-u", "test"]),
        "commit edit",
    );
    assert_ok_with_log(&hg_ssh(&dest, &["push"], &shim), "push", &log);
    (server, shim, log)
}

#[test]
fn tree_repo_push_round_trips() {
    let temp = TempDir::new("tp");
    let server = push_tree_edit(&temp, |dest| {
        fs::create_dir_all(dest.join("new").join("inner")).expect("mkdir new");
        fs::write(dest.join("new").join("inner").join("n"), b"n\n").expect("write n");
        fs::write(dest.join("a").join("deep").join("z"), b"3\n").expect("write z");
        assert_hg(&hg(dest, &["rm", "-q", "B"]), "hg rm B");
    });
    assert_same_tip(&temp.0.join("repo"), &temp.0.join("dest"));
    drop(server);
}

#[test]
fn tree_repo_with_encoded_dir_names_round_trips() {
    let temp = TempDir::new("te");
    let long: String = std::iter::once('L')
        .chain(std::iter::repeat('o').take(199))
        .collect();
    let (server, shim, log) = push_tree_edit(&temp, |dest| {
        fs::create_dir_all(dest.join("Upper").join("Case")).expect("mkdir Upper/Case");
        fs::write(dest.join("Upper").join("Case").join("f"), b"u\n").expect("write u");
        fs::create_dir_all(dest.join(&long)).expect("mkdir long");
        fs::write(dest.join(&long).join("f"), b"l\n").expect("write long");
    });
    let dest = temp.0.join("dest");
    assert_same_tip(&temp.0.join("repo"), &dest);
    let clone = hg_ssh(&temp.0, &["clone", "ssh://user@dummy/repo", "again"], &shim);
    assert_ok_with_log(&clone, "stream clone", &log);
    let stdout = String::from_utf8_lossy(&clone.stdout);
    assert!(stdout.contains("streaming all changes"), "{stdout}");
    assert_same_tip(&temp.0.join("again"), &dest);
    drop(server);
}

#[test]
fn tree_push_into_empty_flat_repo_adds_requirement() {
    let temp = TempDir::new("tf");
    fs::create_dir_all(temp.0.join("src")).expect("mkdir src");
    let source = init_tree_repo(&temp.0.join("src"));
    assert_hg(&hg(&temp.0, &["init", "empty"]), "hg init");
    let repo = temp.0.join("empty");
    let store_requires = repo.join(".hg").join("store").join("requires");
    let before = fs::read_to_string(&store_requires).expect("store requires");
    assert!(!requirements(&repo).lines().any(|line| line == "treemanifest"));
    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let shim = write_ssh_shim(&temp.0, &sibling_bin("hgcli"), &repo);
    let push = hg_ssh(&source, &["push", "ssh://user@dummy/empty"], &shim);
    assert_ok_with_log(&push, "push", &log);
    let after = fs::read_to_string(&store_requires).expect("store requires");
    assert_eq!(after, format!("{before}treemanifest\n"));
    assert_same_tip(&repo, &source);
    drop(server);
}

#[test]
fn tree_push_into_flat_repo_is_refused() {
    let temp = TempDir::new("tr");
    fs::create_dir_all(temp.0.join("src")).expect("mkdir src");
    let source = init_tree_repo(&temp.0.join("src"));
    let repo = init_share_safe_repo(&temp.0);
    let changelog = repo.join(".hg").join("store").join("00changelog.i");
    let before = fs::read(&changelog).expect("changelog");
    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let shim = write_ssh_shim(&temp.0, &sibling_bin("hgcli"), &repo);
    let push = hg_ssh(&source, &["push", "-f", "ssh://user@dummy/repo"], &shim);
    let stderr = String::from_utf8_lossy(&push.stderr);
    assert!(!push.status.success(), "push succeeded:\n{stderr}");
    assert!(
        stderr.contains(
            "bundle contains tree manifests, but local repo is non-empty and does not use tree manifests"
        ),
        "{stderr}\nmononoke:\n{}",
        read_log(&log)
    );
    assert!(fs::read(&changelog).expect("changelog") == before, "changelog changed");
    drop(server);
}

#[test]
fn flat_client_push_into_tree_repo_matches_hg() {
    flat_push_into_tree_repo_matches_hg(false);
}

#[test]
fn bundle1_flat_client_push_into_tree_repo_matches_hg() {
    flat_push_into_tree_repo_matches_hg(true);
}

fn flat_push_into_tree_repo_matches_hg(legacy: bool) {
    let temp = TempDir::new("tc");
    init_share_safe_repo(&temp.0);
    assert_hg(&hg(&temp.0, &["clone", "-q", "repo", "client"]), "flat clone");
    let client = temp.0.join("client");
    for name in ["mono", "stock"] {
        assert_hg(
            &hg(&temp.0, &["init", name, "--config", "experimental.treemanifest=1"]),
            "hg init tree",
        );
    }
    let log = temp.0.join("mononoke.log");
    let mono = Running::start(&temp.0.join("mono"), &log);
    let stock = HgServe::start(&temp.0.join("stock"));
    for port in [mono.port, stock.port] {
        let url = format!("http://127.0.0.1:{port}/");
        let mut args = vec!["push", &url];
        if legacy {
            args.extend(["--config", "devel.legacy.exchange=bundle1"]);
        }
        assert_ok_with_log(&hg(&client, &args), "push", &log);
    }
    drop(mono);
    drop(stock);
    let mut outputs = Vec::new();
    for name in ["mono", "stock"] {
        let repo = temp.0.join(name);
        let verify = hg(&repo, &["verify"]);
        assert_hg(&verify, &format!("{name} verify"));
        let index = hg(&repo, &["debugindex", "-m"]);
        assert_hg(&index, &format!("{name} debugindex"));
        outputs.push((
            String::from_utf8_lossy(&verify.stdout).into_owned(),
            String::from_utf8_lossy(&index.stdout).into_owned(),
        ));
    }
    assert_eq!(outputs[0], outputs[1]);
}

/// Flat repo with a merge and a rename across `a.txt`, `b.txt` and `c.txt`.
///
/// 0 adds a and b, 1 edits a, 2 (from 0) edits b, 3 merges, 4 renames a to c and edits b.
fn init_merge_rename_repo(root: &Path) -> PathBuf {
    init_merge_rename_repo_with_files(root, 0)
}

fn init_merge_rename_repo_with_files(root: &Path, unchanged_files: usize) -> PathBuf {
    let repo = root.join("repo");
    let mut init = Command::new("hg");
    init.current_dir(root)
        .args(["init", "repo"])
        .args(["--config", "format.use-persistent-nodemap=false"])
        .env("HGPLAIN", "1")
        .env("HGRCPATH", "");
    assert_hg(
        &output_timeout(&mut init, Duration::from_secs(120)),
        "hg init",
    );
    let lines = |tag: &str| -> String { (0..40).map(|i| format!("{tag} line {i}\n")).collect() };
    let commit = |msg: &str| {
        assert_hg(
            &hg(&repo, &["commit", "-Aqm", msg, "-u", "test", "-d", "0 0"]),
            msg,
        )
    };
    fs::write(repo.join("a.txt"), lines("a")).expect("write a");
    fs::write(repo.join("b.txt"), lines("b")).expect("write b");
    for i in 0..unchanged_files {
        fs::write(repo.join(format!("unchanged-{i}.txt")), b"unchanged\n").expect("unchanged file");
    }
    commit("zero");
    fs::write(repo.join("a.txt"), lines("a") + "a more\n").expect("edit a");
    commit("one");
    assert_hg(&hg(&repo, &["update", "-q", "0"]), "update 0");
    fs::write(repo.join("b.txt"), lines("b") + "b more\n").expect("edit b");
    commit("two");
    assert_hg(&hg(&repo, &["merge", "-q", "1"]), "merge");
    commit("three");
    assert_hg(&hg(&repo, &["mv", "a.txt", "c.txt"]), "mv");
    fs::write(repo.join("b.txt"), "b changed\n".to_string() + &lines("b")).expect("edit b");
    commit("four");
    repo
}

fn node_of(repo: &Path, rev: &str) -> String {
    let out = hg(repo, &["log", "-r", rev, "-T", "{node}"]);
    assert_hg(&out, "hg log");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn init_names_repo(root: &Path) -> PathBuf {
    let repo = init_zstd_repo(root);
    for args in [
        vec!["tag", "v1.0", "-r", "0", "-u", "test"],
        vec!["branch", "stable"],
        vec!["commit", "-m", "stable", "-u", "test"],
        vec!["tag", "moved", "-r", "0", "-u", "test"],
        vec!["tag", "-f", "moved", "-r", "2", "-u", "test"],
        vec!["commit", "--close-branch", "-m", "closed", "-u", "test"],
        vec!["update", "-r", "2"],
    ] {
        assert_hg(&hg(&repo, &args), "names fixture");
    }
    fs::write(repo.join("open"), b"open").unwrap();
    assert_hg(&hg(&repo, &["add", "open"]), "add open");
    assert_hg(
        &hg(&repo, &["commit", "-m", "open", "-u", "test"]),
        "open head",
    );
    assert_hg(&hg(&repo, &["bookmark", "bookmark"]), "bookmark");
    assert_hg(&hg(&repo, &["update", "-r", "0"]), "update root");
    assert_hg(&hg(&repo, &["branch", "feature x"]), "feature branch");
    assert_hg(
        &hg(&repo, &["commit", "-m", "feature", "-u", "test"]),
        "feature",
    );
    assert_hg(&hg(&repo, &["branch", "closed"]), "closed branch");
    assert_hg(
        &hg(&repo, &["commit", "-m", "closed base", "-u", "test"]),
        "closed base",
    );
    assert_hg(
        &hg(
            &repo,
            &["commit", "--close-branch", "-m", "closed", "-u", "test"],
        ),
        "closed branch",
    );
    assert_hg(
        &hg(&repo, &["tag", "--local", "local", "-r", "0"]),
        "local tag",
    );
    repo
}

#[test]
fn lookup_matches_hg_serve() {
    let temp = TempDir::new("names-lookup");
    let repo = init_names_repo(&temp.0);
    assert_hg(
        &hg(&repo, &["debugbuilddag", "--from-existing", "+32"]),
        "prefix collisions",
    );
    let log = temp.0.join("mononoke.log");
    let mono = Running::start(&repo, &log);
    let stock = HgServe::start(&repo);
    let prefix = node_of(&repo, "2")[..6].to_string();
    let nodes = hg(&repo, &["log", "-T", "{node}\n"]).stdout;
    let digit = b"0123456789abcdef"
        .iter()
        .find(|&&b| {
            nodes
                .split(|c| *c == b'\n')
                .filter(|n| n.first() == Some(&b))
                .count()
                > 1
        })
        .unwrap();
    let ambiguous = format!("x{}", char::from(*digit));
    for key in [
        ".", "tip", "null", "0", "-1", "v1.0", "moved", "local", "stable", "closed", "bookmark",
        &prefix, &ambiguous, "nosuch",
    ] {
        let query = format!("cmd=lookup&key={key}");
        assert_eq!(
            raw_command(mono.port, &query),
            raw_command(stock.port, &query),
            "lookup {key}"
        );
    }
    // Fresh local tags and working-copy parents do not change the changelog stamp.
    assert_hg(&hg(&repo, &["update", "-r", "0"]), "update while serving");
    assert_hg(
        &hg(&repo, &["tag", "--local", "fresh", "-r", "2"]),
        "local while serving",
    );
    let parent = node_of(&repo, ".");
    assert_eq!(parent, node_of(&repo, "0"));
    assert_eq!(
        raw_command(mono.port, "cmd=lookup&key=."),
        format!("1 {parent}\n").as_bytes(),
        "live working-copy parent"
    );
    assert_eq!(
        raw_command(mono.port, "cmd=lookup&key=fresh"),
        format!("1 {}\n", node_of(&repo, "2")).as_bytes(),
        "live local tag"
    );
    // cachedlocalrepo does not watch dirstate/localtags. Refresh only the
    // reference server before comparing current working-copy metadata.
    drop(stock);
    let stock = HgServe::start(&repo);
    for key in [".", "fresh"] {
        let query = format!("cmd=lookup&key={key}");
        assert_eq!(
            raw_command(mono.port, &query),
            raw_command(stock.port, &query),
            "live lookup {key}"
        );
    }
    assert_hg(
        &hg(&repo, &["tag", "--local", "stable", "-r", "0"]),
        "tag shadows branch",
    );
    // cachedlocalrepo watches changelog/bookmarks, not localtags; recreate
    // the stock reference to compare current namespace precedence.
    drop(stock);
    let stock = HgServe::start(&repo);
    let query = "cmd=lookup&key=stable";
    assert_eq!(
        raw_command(mono.port, query),
        raw_command(stock.port, query),
        "namespace precedence after local tag"
    );
    assert_eq!(
        raw_command(mono.port, query),
        format!("1 {}\n", node_of(&repo, "0")).as_bytes()
    );
    assert_hg(
        &hg(&repo, &["bookmark", "-f", "-r", "2", "stable"]),
        "bookmark shadows tag",
    );
    drop(stock);
    let stock = HgServe::start(&repo);
    assert_eq!(
        raw_command(mono.port, query),
        raw_command(stock.port, query),
        "namespace precedence after bookmark"
    );
    assert_eq!(
        raw_command(mono.port, query),
        format!("1 {}\n", node_of(&repo, "2")).as_bytes()
    );
}

#[test]
fn lookup_ignores_missing_global_tag_targets() {
    let temp = TempDir::new("names-missing-target");
    let repo = init_zstd_repo(&temp.0);
    assert_hg(&hg(&repo, &["branch", "stable"]), "stable branch");
    assert_hg(
        &hg(&repo, &["commit", "-m", "stable", "-u", "test"]),
        "stable commit",
    );
    let root = node_of(&repo, "0");
    let prefix = format!("x{}", &root[..6]);
    let missing = "ab".repeat(20);
    fs::write(
        repo.join(".hgtags"),
        format!("{missing} broken\n{missing} stable\n{missing} {prefix}\n"),
    )
    .unwrap();
    assert_hg(&hg(&repo, &["add", ".hgtags"]), "add missing-target tags");
    assert_hg(
        &hg(
            &repo,
            &["commit", "-m", "missing tag targets", "-u", "test"],
        ),
        "commit missing-target tags",
    );
    let log = temp.0.join("mononoke.log");
    let mono = Running::start(&repo, &log);
    let stock = HgServe::start(&repo);
    for key in ["broken", "stable", &prefix] {
        let query = format!("cmd=lookup&key={key}");
        let want = raw_command(stock.port, &query);
        let expected = if key == prefix {
            format!("1 {root}\n")
        } else {
            format!("0 unknown revision '{key}'\n")
        };
        assert_eq!(want, expected.as_bytes(), "stock {key}");
        assert_eq!(raw_command(mono.port, &query), want, "missing target {key}");
    }
}

fn parsed_branchmap(
    bytes: &[u8],
) -> std::collections::BTreeMap<Vec<u8>, std::collections::BTreeSet<Vec<u8>>> {
    bytes
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            let mut parts = line.split(|b| *b == b' ');
            (
                parts.next().unwrap().to_vec(),
                parts.map(<[u8]>::to_vec).collect(),
            )
        })
        .collect()
}

#[test]
fn branch_names_with_spaces_are_quoted() {
    let temp = TempDir::new("names-branchmap");
    let repo = init_names_repo(&temp.0);
    // A legacy Latin-1 branch name exercises byte-preserving lookup parsing.
    let create = "from mercurial import context; repo.commitctx(context.memctx(repo, [repo[b'tip'].node(), repo.nullid], b'raw branch', [], lambda *args: None, user=b'test', extra={b'branch': b'raw\\xff%+/' }))";
    assert_hg(&hg(&repo, &["debugshell", "-c", create]), "raw branch");
    let log = temp.0.join("mononoke.log");
    let mono = Running::start(&repo, &log);
    let stock = HgServe::start(&repo);
    let got = raw_command(mono.port, "cmd=branchmap");
    assert_eq!(
        parsed_branchmap(&got),
        parsed_branchmap(&raw_command(stock.port, "cmd=branchmap"))
    );
    assert!(!got.ends_with(b"\n"));
    assert!(got.windows(11).any(|s| s == b"feature%20x"));
    // This hg/Python build rejects non-ASCII query values in parse_qsl;
    // exercise successful lookup through its byte-preserving SSH transport.
    let mut answers = Vec::new();
    for (label, bin) in [
        ("stock", PathBuf::from("hg")),
        ("mono", sibling_bin("hgcli")),
    ] {
        let shim_dir = temp.0.join(label);
        fs::create_dir(&shim_dir).unwrap();
        let shim = write_ssh_shim(&shim_dir, &bin, &repo);
        let out = hg_ssh(
            &temp.0,
            &["identify", "-i", "-r", "raw\u{ff}%+/", "ssh://dummy/repo"],
            &shim,
        );
        assert_hg(&out, "SSH legacy-name lookup");
        answers.push(out.stdout);
    }
    assert_eq!(answers[0], answers[1]);
    let dest = temp.0.join("pull-dest");
    assert_hg(&hg(&temp.0, &["init", "pull-dest"]), "init pull");
    let url = format!("http://127.0.0.1:{}/", mono.port);
    assert_hg(
        &hg(&dest, &["pull", "-r", "feature x", &url]),
        "pull feature",
    );
    assert_eq!(node_of(&dest, "tip"), node_of(&repo, "feature x"));
    assert_hg(
        &hg(&dest, &["pull", "-r", "raw\u{ff}%+/", &url]),
        "pull normalized legacy name",
    );
    assert_eq!(node_of(&dest, "tip"), node_of(&repo, "tip"));
}

#[test]
fn branch_cache_tracks_pushes_commits_and_strip() {
    let temp = TempDir::new("names-reload");
    let repo = init_zstd_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let mono = Running::start(&repo, &log);
    let stock = HgServe::start(&repo);
    let check = |stock_port| {
        assert_eq!(
            parsed_branchmap(&raw_command(mono.port, "cmd=branchmap")),
            parsed_branchmap(&raw_command(stock_port, "cmd=branchmap"))
        )
    };
    check(stock.port);
    assert_hg(&hg(&repo, &["branch", "stable"]), "outside branch");
    assert_hg(
        &hg(&repo, &["commit", "-m", "stable", "-u", "test"]),
        "outside commit",
    );
    check(stock.port);
    assert_hg(&hg(&repo, &["branch", "-f", "default"]), "re-enter branch");
    assert_hg(
        &hg(&repo, &["commit", "-m", "default", "-u", "test"]),
        "transition commit",
    );
    check(stock.port);
    drop(stock);
    // Windows keeps mapped revlogs open, so strip an independent copy and
    // replace the changelog atomically while Mononoke retains its old view.
    assert_hg(
        &hg(&temp.0, &["clone", "--pull", "-U", "repo", "stripped"]),
        "offline strip clone",
    );
    let stripped = temp.0.join("stripped");
    assert_hg(
        &hg(
            &stripped,
            &[
                "--config",
                "extensions.strip=",
                "strip",
                "--no-backup",
                "-r",
                "1",
            ],
        ),
        "outside strip",
    );
    for name in ["00changelog.i", "00changelog.d"] {
        let old = repo.join(".hg/store").join(name);
        if old.exists() {
            fs::rename(&old, repo.join(format!("old-{name}"))).unwrap();
        }
        let replacement = stripped.join(".hg/store").join(name);
        if replacement.exists() {
            fs::copy(replacement, old).unwrap();
        }
    }
    let stock = HgServe::start(&repo);
    check(stock.port);
    let url = format!("http://127.0.0.1:{}/", mono.port);
    assert_hg(
        &hg(&temp.0, &["clone", &url, "client"]),
        "clone push client",
    );
    let client = temp.0.join("client");
    assert_hg(&hg(&client, &["branch", "pushed"]), "pushed branch");
    assert_hg(
        &hg(&client, &["commit", "-m", "pushed", "-u", "test"]),
        "pushed commit",
    );
    assert_hg(
        &hg(&client, &["push", "--new-branch", &url]),
        "push new branch",
    );
    check(stock.port);
    assert_eq!(
        raw_command(mono.port, "cmd=lookup&key=pushed"),
        raw_command(stock.port, "cmd=lookup&key=pushed")
    );
}

#[test]
fn lookup_dirstate_v2_matches_hg_serve() {
    let temp = TempDir::new("names-dsv2");
    let repo = init_zstd_repo(&temp.0);
    assert_hg(
        &hg(
            &repo,
            &[
                "debugupgraderepo",
                "--run",
                "--config",
                "format.use-dirstate-v2=true",
                "--config",
                "storage.dirstate-v2.slow-path=allow",
            ],
        ),
        "upgrade dirstate",
    );
    fs::write(
        repo.join(".hg/hgrc"),
        b"[storage]\ndirstate-v2.slow-path=allow\n",
    )
    .unwrap();
    let log = temp.0.join("mononoke.log");
    let mono = Running::start(&repo, &log);
    let stock = HgServe::start(&repo);
    assert_eq!(
        raw_command(mono.port, "cmd=lookup&key=."),
        raw_command(stock.port, "cmd=lookup&key=.")
    );
}

#[test]
fn branches_matches_hg_serve() {
    let temp = TempDir::new("names-branches");
    let repo = init_merge_rename_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let mono = Running::start(&repo, &log);
    let stock = HgServe::start(&repo);
    for query in [
        "cmd=branches&nodes=".to_string(),
        format!(
            "cmd=branches&nodes={}%20{}",
            node_of(&repo, "tip"),
            node_of(&repo, "3")
        ),
        format!("cmd=branches&nodes={}", "0".repeat(40)),
    ] {
        assert_eq!(
            raw_command(mono.port, &query),
            raw_command(stock.port, &query),
            "{query}"
        );
    }
}

#[test]
fn clone_r_tag_and_branch() {
    let temp = TempDir::new("names-clone");
    let repo = init_names_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let mono = Running::start(&repo, &log);
    let url = format!("http://127.0.0.1:{}/", mono.port);
    for (key, dest) in [
        ("v1.0", "tag-dest"),
        ("stable", "branch-dest"),
        (".", "dot-dest"),
    ] {
        assert_hg(
            &hg(&temp.0, &["clone", "-r", key, &url, dest]),
            "clone named revision",
        );
        let want = hg(
            &repo,
            &[
                "log",
                "-r",
                &format!("ancestors('{key}')"),
                "-T",
                "{node}\n",
            ],
        );
        assert_hg(&want, "ancestor nodes");
        let got = hg(
            &temp.0.join(dest),
            &["log", "-r", "sort(all(), rev)", "-T", "{node}\n"],
        );
        assert_hg(&got, "clone nodes");
        assert_eq!(got.stdout, want.stdout, "clone {key}");
        assert_hg(&hg(&temp.0.join(dest), &["verify"]), "verify named clone");
    }
}

/// `GET /?<query>` with `Connection: close`. Returns the body, zlib-inflated
/// when it is a stream response (`application/mercurial-0.1`).
fn raw_command(port: u16, query: &str) -> Vec<u8> {
    use std::io::{Read, Write};
    let mut sock = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
    sock.set_read_timeout(Some(Duration::from_secs(30)))
        .expect("read timeout");
    let method = if query.starts_with("cmd=unbundle&") {
        "POST"
    } else {
        "GET"
    };
    write!(
        sock,
        "{method} /?{query} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAccept: application/mercurial-0.1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    )
    .expect("send request");
    let mut raw = Vec::new();
    sock.read_to_end(&mut raw).expect("read response");
    let split = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("end of headers");
    let head = String::from_utf8_lossy(&raw[..split]).to_ascii_lowercase();
    let mut rest = &raw[split + 4..];
    assert!(
        head.starts_with("http/1.1 200") || head.starts_with("http/1.0 200"),
        "{query}: {head}"
    );
    let header = |name: &str| {
        head.lines()
            .find_map(|line| line.strip_prefix(&format!("{name}:")))
            .map(|value| value.trim().to_string())
    };
    let body = if header("transfer-encoding").as_deref() == Some("chunked") {
        let mut body = Vec::new();
        loop {
            let end = rest
                .windows(2)
                .position(|w| w == b"\r\n")
                .expect("chunk size");
            let size_text = String::from_utf8_lossy(&rest[..end]).into_owned();
            let size = usize::from_str_radix(size_text.split(';').next().unwrap().trim(), 16)
                .expect("chunk size hex");
            rest = &rest[end + 2..];
            if size == 0 {
                break body;
            }
            body.extend_from_slice(&rest[..size]);
            rest = &rest[size + 2..];
        }
    } else if let Some(len) = header("content-length") {
        rest[..len.parse::<usize>().expect("content-length")].to_vec()
    } else {
        rest.to_vec()
    };
    let cmd = query
        .split('&')
        .find_map(|kv| kv.strip_prefix("cmd="))
        .unwrap_or("");
    let stream = matches!(cmd, "changegroup" | "changegroupsubset" | "getbundle");
    if stream && header("content-type").as_deref() == Some("application/mercurial-0.1") {
        let mut out = Vec::new();
        flate2::read::ZlibDecoder::new(&body[..])
            .read_to_end(&mut out)
            .expect("inflate");
        out
    } else {
        body
    }
}

/// Each query's bytes from Mononoke and from stock `hg serve` on the same repo.
fn assert_same_as_hg_serve(repo: &Path, temp: &TempDir, queries: &[String]) {
    let log = temp.0.join("mononoke.log");
    let mono = Running::start(repo, &log);
    let stock = HgServe::start(repo);
    for query in queries {
        let want = raw_command(stock.port, query);
        let got = raw_command(mono.port, query);
        assert!(want.len() >= 12, "{query}: hg sent {} bytes", want.len());
        let first_difference = got
            .iter()
            .zip(&want)
            .position(|(a, b)| a != b)
            .unwrap_or(got.len().min(want.len()));
        assert!(
            got == want,
            "{query}: {} bytes from mononoke, {} from hg, first difference at {first_difference}\nlog:\n{}",
            got.len(),
            want.len(),
            read_log(&log)
        );
    }
}

#[test]
fn changegroup_matches_hg_serve() {
    let temp = TempDir::new("cg1");
    let repo = init_merge_rename_repo(&temp.0);
    let queries = [
        format!("cmd=changegroup&roots={}", "0".repeat(40)),
        format!("cmd=changegroup&roots={}", node_of(&repo, "2")),
        format!(
            "cmd=changegroupsubset&bases={}&heads={}",
            node_of(&repo, "1"),
            node_of(&repo, "4")
        ),
    ];
    assert_same_as_hg_serve(&repo, &temp, &queries);
}

#[test]
fn bundle1_getbundle_matches_hg_serve() {
    let temp = TempDir::new("gb1");
    let repo = init_merge_rename_repo(&temp.0);
    let queries = [format!(
        "cmd=getbundle&heads={}&common={}",
        node_of(&repo, "tip"),
        node_of(&repo, "1")
    )];
    assert_same_as_hg_serve(&repo, &temp, &queries);
}

#[test]
fn changegroup_empty_and_null_selections_match_hg_serve() {
    let temp = TempDir::new("cg1empty");
    let repo = init_merge_rename_repo(&temp.0);
    let null = "0".repeat(40);
    let queries = [
        format!("cmd=changegroupsubset&bases={null}&heads="),
        format!("cmd=changegroupsubset&bases={null}&heads={null}"),
        format!("cmd=getbundle&heads={null}"),
    ];
    assert_same_as_hg_serve(&repo, &temp, &queries);
}

#[test]
fn restricted_legacy_pull_sends_nodes_reused_from_a_sibling() {
    let temp = TempDir::new("cg1reuse");
    let repo = init_merge_rename_repo_with_files(&temp.0, 40);
    assert_hg(&hg(&repo, &["update", "-q", "0"]), "update base");
    let lines: String = (0..40).map(|i| format!("a line {i}\n")).collect();
    fs::write(repo.join("a.txt"), lines + "a more\n").expect("same edit as rev1");
    assert_hg(
        &hg(
            &repo,
            &["commit", "-qm", "sibling", "-u", "test", "-d", "0 0"],
        ),
        "commit sibling",
    );
    let tip = node_of(&repo, "tip");
    fs::write(repo.join("b.txt"), b"child b\n").expect("edit b");
    assert_hg(
        &hg(
            &repo,
            &["commit", "-qm", "child b", "-u", "test", "-d", "0 0"],
        ),
        "commit child b",
    );
    fs::write(repo.join("a.txt"), b"child a\n").expect("edit a");
    assert_hg(
        &hg(
            &repo,
            &["commit", "-qm", "child a", "-u", "test", "-d", "0 0"],
        ),
        "commit child a",
    );
    let descendant = node_of(&repo, "tip");
    let queries = [
        format!("cmd=changegroupsubset&bases={}&heads={tip}", "0".repeat(40)),
        format!("cmd=getbundle&heads={tip}"),
        format!("cmd=getbundle&heads={descendant}&common={tip}"),
    ];
    assert_same_as_hg_serve(&repo, &temp, &queries);
}

#[test]
fn ssh_legacy_pull_works() {
    let temp = TempDir::new("lp");
    let repo = init_merge_rename_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let shim = write_ssh_shim(&temp.0, &sibling_bin("hgcli"), &repo);
    assert_hg(&hg(&temp.0, &["init", "dest"]), "hg init dest");
    let dest = temp.0.join("dest");
    // `-r` keeps the client from a legacy `stream_out` clone into the empty repo.
    let pull = hg_ssh(
        &dest,
        &[
            "pull",
            "-r",
            "tip",
            "ssh://user@dummy/repo",
            "--config",
            "devel.legacy.exchange=bundle1",
        ],
        &shim,
    );
    assert_ok_with_log(&pull, "legacy pull", &log);
    assert_hg(&hg(&dest, &["verify"]), "verify");
    assert_eq!(node_of(&dest, "tip"), node_of(&repo, "tip"));
    drop(server);
}

#[test]
fn stream_out_matches_hg_serve() {
    let temp = TempDir::new("stream-v1");
    let repo = init_merge_rename_repo(&temp.0);
    let dir = repo.join("odd.i").join("nested.d").join("deep.hg");
    fs::create_dir_all(&dir).unwrap();
    // Incompressible data splits the filelog into .d and .i files.
    let mut value = 42u32;
    let large: Vec<u8> = (0..2_100_000)
        .map(|_| {
            value ^= value << 13;
            value ^= value >> 17;
            value ^= value << 5;
            value as u8
        })
        .collect();
    fs::write(dir.join("large"), large).unwrap();
    assert_hg(
        &hg(&repo, &["commit", "-Aqm", "large", "-u", "test"]),
        "large commit",
    );
    fs::write(repo.join(".hg/store/00manifest.d"), b"").unwrap();
    let log = temp.0.join("mono.log");
    let mono = Running::start(&repo, &log);
    let stock = HgServe::start(&repo);
    let want = raw_command(stock.port, "cmd=stream_out");
    assert!(want.starts_with(b"0\n"));
    let got = raw_command(mono.port, "cmd=stream_out");
    assert!(
        got == want,
        "stream bytes differ: got {}, expected {}; first difference {:?}",
        got.len(),
        want.len(),
        got.iter().zip(&want).position(|(a, b)| a != b)
    );
    let shim = write_ssh_shim(&temp.0, &sibling_bin("hgcli"), &repo);
    let clone = hg_ssh(
        &temp.0,
        &[
            "clone",
            "--stream",
            "--config",
            "devel.legacy.exchange=bundle1",
            "ssh://dummy/repo",
            "dest",
        ],
        &shim,
    );
    assert_ok_with_log(&clone, "legacy stream clone", &log);
    assert_hg(
        &hg(&temp.0.join("dest"), &["verify"]),
        "verify legacy stream",
    );
    assert_eq!(node_of(&temp.0.join("dest"), "tip"), node_of(&repo, "tip"));
    assert_eq!(
        fs::read(temp.0.join("dest/odd.i/nested.d/deep.hg/large")).unwrap(),
        fs::read(dir.join("large")).unwrap()
    );
}

#[test]
fn clonebundles_match_hg_serve() {
    let temp = TempDir::new("clone-manifest");
    let repo = init_zstd_repo(&temp.0);
    let log = temp.0.join("mono.log");
    let mono = Running::start(&repo, &log);
    let stock = HgServe::start_with_config(&repo, &["extensions.clonebundles="]);
    let manifest = repo.join(".hg/clonebundles.manifest");
    for contents in [
        None,
        Some(&b""[..]),
        Some(
            &b"https://example/a BUNDLESPEC=zstd-v2\r\npeer-bundle-cache://b.hg BUNDLESPEC=zstd-v2"
                [..],
        ),
        Some(&b"https://example/a BUNDLESPEC=zstd-v2\rpeer-bundle-cache://b.hg BUNDLESPEC=zstd-v2\r"[..]),
    ] {
        if let Some(contents) = contents {
            fs::write(&manifest, contents).unwrap();
        }
        for cmd in ["clonebundles", "clonebundles_manifest"] {
            let query = format!("cmd={cmd}");
            assert_eq!(
                raw_command(mono.port, &query),
                raw_command(stock.port, &query),
                "{cmd} {contents:?}"
            );
        }
        let caps = String::from_utf8(raw_command(mono.port, "cmd=capabilities")).unwrap();
        assert_eq!(
            caps.split(' ').any(|c| c == "clonebundles"),
            contents.is_some()
        );
        assert_eq!(
            caps.split(' ').any(|c| c == "clonebundles_manifest"),
            contents.is_some()
        );
        assert!(!caps.split(' ').any(|c| c == "protocaps"));
    }
    assert_eq!(raw_command(mono.port, "cmd=protocaps&caps=foo%20b"), b"OK");
}

#[test]
fn inline_clone_bundle_clones() {
    let temp = TempDir::new("inline-clonebundle");
    let repo = init_zstd_repo(&temp.0);
    fs::create_dir_all(repo.join(".hg/bundle-cache")).unwrap();
    assert_hg(
        &hg(
            &repo,
            &[
                "bundle",
                "--all",
                "-t",
                "zstd-v2",
                ".hg/bundle-cache/full.hg",
            ],
        ),
        "bundle",
    );
    fs::write(
        repo.join(".hg/clonebundles.manifest"),
        "peer-bundle-cache://full.hg BUNDLESPEC=zstd-v2\n",
    )
    .unwrap();
    let log = temp.0.join("mono.log");
    let mono = Running::start(&repo, &log);
    let shim = write_ssh_shim(&temp.0, &sibling_bin("hgcli"), &repo);
    let url = format!("http://127.0.0.1:{}/", mono.port);
    for (name, source) in [
        ("default", "ssh://dummy/repo"),
        ("http", url.as_str()),
        ("override", "ssh://dummy/repo"),
    ] {
        if name == "override" {
            fs::create_dir_all(repo.join(".hg/bundles")).unwrap();
            fs::copy(
                repo.join(".hg/bundle-cache/full.hg"),
                repo.join(".hg/bundles/full.hg"),
            )
            .unwrap();
            fs::write(
                repo.join(".hg/hgrc"),
                "[server]\npeer-bundle-cache-root = bundles\n[extensions]\nclonebundles =\n",
            )
            .unwrap();
            fs::remove_file(repo.join(".hg/bundle-cache/full.hg")).unwrap();
        }
        let clone = hg_ssh(&temp.0, &["clone", source, name], &shim);
        assert_ok_with_log(&clone, "inline clone", &log);
        let output = String::from_utf8_lossy(&clone.stdout);
        assert!(output.contains("applying clone bundle"), "{output}");
        assert_hg(&hg(&temp.0.join(name), &["verify"]), "verify inline clone");
    }
    fs::write(
        repo.join(".hg/clonebundles.manifest"),
        "peer-bundle-cache://../requires BUNDLESPEC=zstd-v2\n",
    )
    .unwrap();
    let clone = hg_ssh(
        &temp.0,
        &[
            "clone",
            "--config",
            "ui.clonebundlefallback=false",
            "ssh://dummy/repo",
            "illegal",
        ],
        &shim,
    );
    assert!(!clone.status.success());
    let stock_dir = temp.0.join("stock-shim");
    fs::create_dir(&stock_dir).unwrap();
    let stock_shim = write_ssh_shim(&stock_dir, &PathBuf::from("hg"), &repo);
    let stock = hg_ssh(&temp.0, &["clone", "--config", "ui.clonebundlefallback=false", "ssh://dummy/repo", "illegal-stock"], &stock_shim);
    assert!(!stock.status.success());
    // Stock's unframed client reports EOF here; the precise server diagnostic
    // is visible in its error channel/log and in an HTTP error response.
    assert_eq!(clone.stderr, stock.stderr);
    let illegal = raw_command(mono.port, "cmd=get_cached_bundle_inline&path=../requires");
    assert!(String::from_utf8_lossy(&illegal).contains("clonebundle ../requires is using an illegal path"));
    assert!(
        read_log(&log).contains("clonebundle ../requires is using an illegal path"),
        "{}\n{}",
        String::from_utf8_lossy(&clone.stderr),
        read_log(&log)
    );
    let missing = raw_command(mono.port, "cmd=get_cached_bundle_inline&path=absent.hg");
    assert!(String::from_utf8_lossy(&missing).contains("clonebundle absent.hg does not exist"));
}

/// Tracked by `lfs_config`'s size rule.
fn lfs_blob() -> Vec<u8> {
    (0..64 * 1024u32).map(|i| (i ^ (i >> 8)) as u8).collect()
}

/// Client config with a blob cache of its own, so that a clone with its own
/// `cache` can only have its blobs from the server.
fn lfs_config(cache: &Path) -> Vec<String> {
    [
        "extensions.lfs=".to_string(),
        "lfs.track=size(\">1KB\")".to_string(),
        format!("lfs.usercache={}", cfg_path(cache)),
    ]
    .into_iter()
    .flat_map(|config| ["--config".to_string(), config])
    .collect()
}

fn hg_lfs(dir: &Path, cache: &Path, args: &[&str]) -> Output {
    let config = lfs_config(cache);
    let config: Vec<&str> = config.iter().map(String::as_str).collect();
    hg(dir, &[&config[..], args].concat())
}

fn hg_lfs_ssh(dir: &Path, cache: &Path, args: &[&str], shim: &Path) -> Output {
    let config = lfs_config(cache);
    let config: Vec<&str> = config.iter().map(String::as_str).collect();
    hg_ssh(dir, &[&config[..], args].concat(), shim)
}

fn assert_requires_lfs(repo: &Path) {
    let all = requirements(repo);
    assert!(all.lines().any(|line| line == "lfs"), "{all}");
}

/// A clone of `url` with an LFS commit to push.
fn lfs_client(temp: &TempDir, url: &str) -> PathBuf {
    let cache = temp.0.join("client-cache");
    assert_hg(&hg_lfs(&temp.0, &cache, &["clone", url, "client"]), "clone");
    let client = temp.0.join("client");
    fs::write(client.join("big.bin"), lfs_blob()).expect("write big.bin");
    let commit = ["commit", "-Aqm", "big", "-u", "test"];
    assert_hg(&hg_lfs(&client, &cache, &commit), "commit big.bin");
    client
}

fn init_empty_repo(root: &Path) -> PathBuf {
    assert_hg(&hg(root, &["init", "repo"]), "hg init");
    root.join("repo")
}

#[test]
fn http_lfs_push_and_clone() {
    let temp = TempDir::new("lh");
    let repo = init_empty_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let (cert, key) = write_test_cert(&temp.0);
    let (server, https_port) = Running::start_https(&repo, &log, &cert, &key);
    let url = format!("http://127.0.0.1:{}/", server.port);
    let https_url = format!("https://127.0.0.1:{https_port}/");

    let client = lfs_client(&temp, &url);
    let cache = temp.0.join("client-cache");
    let push = hg_lfs(&client, &cache, &["push", &url]);
    assert_ok_with_log(&push, "lfs push", &log);
    assert_requires_lfs(&repo);
    assert_ok_with_log(&hg(&repo, &["verify"]), "served hg verify", &log);

    let cacerts = format!("web.cacerts={}", cfg_path(&cert));
    let clones = [
        ("pull", vec!["clone", "--pull", url.as_str(), "pull"]),
        (
            "https",
            vec!["clone", "--config", &cacerts, https_url.as_str(), "https"],
        ),
    ];
    for (dir, args) in clones {
        let cache = temp.0.join(format!("{dir}-cache"));
        assert_ok_with_log(&hg_lfs(&temp.0, &cache, &args), dir, &log);
        assert_eq!(fs::read(temp.0.join(dir).join("big.bin")).expect(dir), lfs_blob());
    }
    assert_eq!(read_log(&log).matches("LFS GET").count(), 2, "{}", read_log(&log));

    let cache = temp.0.join("stream-cache");
    let clone = hg_lfs(&temp.0, &cache, &["clone", "-U", &url, "stream"]);
    assert_ok_with_log(&clone, "stream clone", &log);
    let stdout = String::from_utf8_lossy(&clone.stdout);
    assert!(stdout.contains("streaming all changes"), "{stdout}");
    let stream = temp.0.join("stream");
    assert_requires_lfs(&stream);
    let store = stream.join(".hg").join("store");
    assert!(!store.join("lfs").exists(), "stream clone sent store/lfs");
    assert_ok_with_log(&hg_lfs(&stream, &cache, &["update"]), "update stream clone", &log);
    assert_eq!(fs::read(stream.join("big.bin")).expect("stream big.bin"), lfs_blob());
    assert_eq!(read_log(&log).matches("LFS GET").count(), 3, "{}", read_log(&log));
    assert_ok_with_log(&hg_lfs(&stream, &cache, &["verify"]), "verify stream clone", &log);
}

#[test]
fn ssh_lfs_push_with_lfs_url() {
    let temp = TempDir::new("ls");
    let repo = init_empty_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let _guard = lock_ports();
    let port = free_port();
    let base = format!("http://127.0.0.1:{port}");
    let public = ["--public-url".to_string(), format!("{base}/")];
    let _server = Running::spawn(&repo, &log, port, &public);
    let shim = write_ssh_shim(&temp.0, &sibling_bin("hgcli"), &repo);
    let lfs_url = format!("lfs.url={base}/.git/info/lfs");
    let remote = "ssh://user@dummy/repo";

    let cache = temp.0.join("client-cache");
    let clone = hg_lfs_ssh(&temp.0, &cache, &["clone", remote, "client"], &shim);
    assert_ok_with_log(&clone, "ssh clone of the empty repo", &log);
    let client = temp.0.join("client");
    fs::write(client.join("big.bin"), lfs_blob()).expect("write big.bin");
    let commit = ["commit", "-Aqm", "big", "-u", "test"];
    assert_hg(&hg_lfs(&client, &cache, &commit), "commit big.bin");
    let push = ["push", "--config", &lfs_url, remote];
    assert_ok_with_log(&hg_lfs_ssh(&client, &cache, &push, &shim), "ssh push", &log);
    assert_requires_lfs(&repo);

    let cache = temp.0.join("dest-cache");
    let clone = ["clone", "--config", &lfs_url, remote, "dest"];
    assert_ok_with_log(&hg_lfs_ssh(&temp.0, &cache, &clone, &shim), "ssh clone", &log);
    assert_eq!(fs::read(temp.0.join("dest").join("big.bin")).expect("dest"), lfs_blob());
    assert!(read_log(&log).contains("LFS GET"), "{}", read_log(&log));
}

#[test]
fn push_with_missing_blob_is_rejected() {
    let temp = TempDir::new("lm");
    let repo = init_zstd_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let url = format!("http://127.0.0.1:{}/", server.port);
    let client = lfs_client(&temp, &url);
    let before = changelog_len(&repo);

    let elsewhere = temp.0.join("elsewhere");
    fs::create_dir(&elsewhere).expect("mkdir elsewhere");
    let lfs_url = format!("lfs.url=file:///{}", cfg_path(&elsewhere).trim_start_matches('/'));
    let cache = temp.0.join("client-cache");
    let push = hg_lfs(&client, &cache, &["push", "--config", &lfs_url, &url]);
    assert!(!push.status.success(), "push without the blob was accepted");
    let stderr = String::from_utf8_lossy(&push.stderr);
    assert!(stderr.contains("is not on the server"), "{stderr}");
    assert_eq!(changelog_len(&repo), before);
}

#[test]
fn plain_file_over_limit_is_rejected() {
    let temp = TempDir::new("lp");
    let repo = init_zstd_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let port = server.port;
    let url = format!("http://127.0.0.1:{port}/");
    assert_hg(&hg(&temp.0, &["clone", &url, "client"]), "clone");
    let client = temp.0.join("client");
    fs::write(client.join("big.bin"), vec![b'x'; 11_000_000]).expect("write big.bin");
    assert_hg(&hg(&client, &["commit", "-Aqm", "big", "-u", "test"]), "commit");

    let push = hg(&client, &["push", &url]);
    assert!(!push.status.success(), "push over the limit was accepted");
    let stderr = String::from_utf8_lossy(&push.stderr);
    assert!(stderr.contains("over the server's 10 MB limit"), "{stderr}");

    drop(server);
    // `spawn` waits for this file, which the killed server left behind.
    let _ = fs::remove_file(repo.join(".hg").join("mononoke.sock"));
    let no_limit = ["--max-plain-file-size".to_string(), "0".to_string()];
    let _server = Running::spawn(&repo, &log, port, &no_limit);
    assert_ok_with_log(&hg(&client, &["push", &url]), "push without a limit", &log);
    assert_hg(&hg(&repo, &["update"]), "update served repo");
    assert_eq!(fs::read(repo.join("big.bin")).expect("big.bin").len(), 11_000_000);
}

#[test]
fn outside_lfs_commit_is_served() {
    let temp = TempDir::new("lo");
    let repo = init_zstd_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let url = format!("http://127.0.0.1:{}/", server.port);
    let caps = || String::from_utf8_lossy(&raw_command(server.port, "cmd=capabilities")).into_owned();
    assert!(caps().split(' ').any(|cap| cap == "lfs"), "{}", caps());
    assert!(!caps().contains("lfs-serve"), "{}", caps());

    fs::write(repo.join("big.bin"), lfs_blob()).expect("write big.bin");
    let cache = temp.0.join("server-cache");
    let commit = ["commit", "-Aqm", "big", "-u", "test"];
    assert_hg(&hg_lfs(&repo, &cache, &commit), "commit in the served repo");
    assert!(caps().contains("lfs-serve"), "{}", caps());

    let cache = temp.0.join("dest-cache");
    assert_ok_with_log(&hg_lfs(&temp.0, &cache, &["clone", &url, "dest"]), "clone", &log);
    assert_eq!(fs::read(temp.0.join("dest").join("big.bin")).expect("dest"), lfs_blob());
    assert!(read_log(&log).contains("LFS GET"), "{}", read_log(&log));
}

#[test]
fn lfs_file_renamed_from_a_non_utf8_name_round_trips() {
    let temp = TempDir::new("lr");
    let repo = init_empty_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let server = Running::start(&repo, &log);
    let url = format!("http://127.0.0.1:{}/", server.port);
    let client = lfs_client(&temp, &url);
    let cache = temp.0.join("client-cache");

    // Latin-1 on unix. Windows hg stores paths in the ANSI code page.
    #[cfg(unix)]
    let name = {
        use std::os::unix::ffi::OsStrExt;
        std::ffi::OsStr::from_bytes(b"caf\xe9.bin").to_owned()
    };
    #[cfg(not(unix))]
    let name = std::ffi::OsString::from("caf\u{e9}.bin");
    let addremove = ["addremove", "-q", "-s", "100"];
    let commit = ["commit", "-qm", "rename", "-u", "test"];
    fs::rename(client.join("big.bin"), client.join(&name)).expect("rename to the odd name");
    assert_hg(&hg_lfs(&client, &cache, &addremove), "addremove");
    assert_hg(&hg_lfs(&client, &cache, &commit), "commit odd name");
    fs::rename(client.join(&name), client.join("plain.bin")).expect("rename back");
    assert_hg(&hg_lfs(&client, &cache, &addremove), "addremove");
    assert_hg(&hg_lfs(&client, &cache, &commit), "commit rename");

    let pointer = hg_lfs(&client, &cache, &["debugdata", "plain.bin", "0"]);
    assert_hg(&pointer, "debugdata");
    let pointer = pointer.stdout;
    assert!(
        pointer.windows(10).any(|bytes| bytes == b"x-hg-copy "),
        "no rename in {}",
        String::from_utf8_lossy(&pointer)
    );
    assert!(
        std::str::from_utf8(&pointer).is_err(),
        "the name is utf-8 here: {}",
        String::from_utf8_lossy(&pointer)
    );

    assert_ok_with_log(&hg_lfs(&client, &cache, &["push", &url]), "push rename", &log);
    assert_ok_with_log(&hg(&repo, &["verify"]), "served hg verify", &log);
    let cache = temp.0.join("dest-cache");
    assert_ok_with_log(&hg_lfs(&temp.0, &cache, &["clone", &url, "dest"]), "clone", &log);
    assert_eq!(fs::read(temp.0.join("dest").join("plain.bin")).expect("dest"), lfs_blob());
}

fn run_server(args: &[&str]) -> Output {
    let mut cmd = Command::new(sibling_bin("mononoke"));
    cmd.args(args);
    output_timeout(&mut cmd, Duration::from_secs(30))
}

/// Pushes and clones an LFS file through a server whose blobs live in a
/// bucket, and checks that nothing reached the repo and no batch response names
/// the bucket. `hidden` are the strings that must stay out of those responses.
fn lfs_bucket_round_trip(store: &str, endpoint: Option<&str>, hidden: &[&str]) {
    use std::io::{Read, Write};
    let temp = TempDir::new("lb");
    let repo = init_empty_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let mut extra = vec!["--lfs-store".to_string(), store.to_string()];
    if let Some(endpoint) = endpoint {
        extra.extend(["--lfs-endpoint".to_string(), endpoint.to_string()]);
    }
    let _guard = lock_ports();
    let port = free_port();
    let server = Running::spawn(&repo, &log, port, &extra);
    drop(_guard);
    let url = format!("http://127.0.0.1:{}/", server.port);

    let client = lfs_client(&temp, &url);
    let cache = temp.0.join("client-cache");
    assert_ok_with_log(&hg_lfs(&client, &cache, &["push", &url]), "lfs push", &log);
    assert_requires_lfs(&repo);

    let clone_cache = temp.0.join("clone-cache");
    let clone = hg_lfs(&temp.0, &clone_cache, &["clone", &url, "dest"]);
    assert_ok_with_log(&clone, "lfs clone", &log);
    assert_eq!(fs::read(temp.0.join("dest").join("big.bin")).expect("dest"), lfs_blob());

    let objects = repo.join(".hg").join("store").join("lfs").join("objects");
    let local = fs::read_dir(&objects).map(|dir| dir.count()).unwrap_or(0);
    assert_eq!(local, 0, "a blob reached the served repo");

    let pointer = hg_lfs(&client, &cache, &["debugdata", "big.bin", "0"]);
    let pointer = String::from_utf8_lossy(&pointer.stdout).into_owned();
    let oid = pointer.split("oid sha256:").nth(1).and_then(|rest| rest.split_whitespace().next());
    let oid = oid.unwrap_or_else(|| panic!("no oid in {pointer}"));
    let json = format!(
        r#"{{"operation":"download","objects":[{{"oid":"{oid}","size":{}}}]}}"#,
        lfs_blob().len()
    );
    let mut sock = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
    sock.set_read_timeout(Some(Duration::from_secs(30))).expect("read timeout");
    write!(
        sock,
        "POST /.git/info/lfs/objects/batch HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\
         Accept: application/vnd.git-lfs+json\r\nContent-Type: application/vnd.git-lfs+json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{json}",
        json.len()
    )
    .expect("send batch");
    let mut raw = Vec::new();
    sock.read_to_end(&mut raw).expect("read batch response");
    let body = String::from_utf8_lossy(&raw);
    assert!(body.starts_with("HTTP/1.1 200"), "{body}");
    assert!(body.contains(oid), "{body}");
    for secret in hidden {
        assert!(!body.contains(secret), "batch response names {secret}: {body}");
    }
}

fn unique_prefix() -> String {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_nanos();
    format!("mononoke-test-{nanos}")
}

#[test]
fn s3_lfs_push_and_clone() {
    let (Ok(endpoint), Ok(bucket)) = (
        std::env::var("MONONOKE_TEST_S3_ENDPOINT"),
        std::env::var("MONONOKE_TEST_S3_BUCKET"),
    ) else {
        println!("skipped: MONONOKE_TEST_S3_ENDPOINT and MONONOKE_TEST_S3_BUCKET are not set");
        return;
    };
    let host = endpoint.split("://").last().unwrap_or(&endpoint).trim_end_matches('/');
    let store = format!("s3://{bucket}/{}", unique_prefix());
    lfs_bucket_round_trip(&store, Some(&endpoint), &[&bucket, host]);
}

#[test]
fn azure_lfs_push_and_clone() {
    let (Ok(account), Ok(container)) = (
        std::env::var("MONONOKE_TEST_AZURE_ACCOUNT"),
        std::env::var("MONONOKE_TEST_AZURE_CONTAINER"),
    ) else {
        println!("skipped: MONONOKE_TEST_AZURE_ACCOUNT and MONONOKE_TEST_AZURE_CONTAINER are not set");
        return;
    };
    let store = format!("azure://{account}/{container}/{}", unique_prefix());
    lfs_bucket_round_trip(&store, None, &[&account, &container]);
}

#[test]
fn second_repo_with_lfs_is_refused() {
    let temp = TempDir::new("l2");
    fs::create_dir(temp.0.join("plain")).expect("mkdir plain");
    let plain = init_zstd_repo(&temp.0.join("plain"));
    let lfs = init_empty_repo(&temp.0);
    fs::write(lfs.join("big.bin"), lfs_blob()).expect("write big.bin");
    let commit = ["commit", "-Aqm", "big", "-u", "test"];
    assert_hg(&hg_lfs(&lfs, &temp.0.join("cache"), &commit), "commit");

    let port = free_port().to_string();
    let server = run_server(&[
        "--http-port",
        &port,
        plain.to_str().expect("utf-8 path"),
        lfs.to_str().expect("utf-8 path"),
    ]);
    assert!(!server.status.success(), "served an lfs repo without HTTP");
    let stderr = String::from_utf8_lossy(&server.stderr);
    assert!(stderr.contains("lfs repositories need HTTP"), "{stderr}");
}

#[test]
fn lfs_endpoint_without_an_s3_store_is_refused() {
    let temp = TempDir::new("le");
    let repo = init_empty_repo(&temp.0);
    let port = free_port().to_string();
    let server = run_server(&[
        "--http-port",
        &port,
        "--lfs-endpoint",
        "http://127.0.0.1:9000",
        repo.to_str().expect("utf-8 path"),
    ]);
    assert!(!server.status.success(), "accepted a useless --lfs-endpoint");
    let stderr = String::from_utf8_lossy(&server.stderr);
    assert!(stderr.contains("--lfs-endpoint needs an s3:// --lfs-store"), "{stderr}");
}

#[test]
fn unreachable_lfs_store_refuses_to_start() {
    let temp = TempDir::new("lu");
    let repo = init_empty_repo(&temp.0);
    let closed = free_port();
    let mut cmd = Command::new(sibling_bin("mononoke"));
    cmd.arg("--http-port")
        .arg(free_port().to_string())
        .args(["--lfs-store", "s3://bucket", "--lfs-endpoint"])
        .arg(format!("http://127.0.0.1:{closed}"))
        .arg(&repo)
        .env("AWS_ACCESS_KEY_ID", "AKIAUNREACHABLE0KEY")
        .env("AWS_SECRET_ACCESS_KEY", "unreachable-secret-value")
        .env("AWS_REGION", "us-east-1");
    let server = output_timeout(&mut cmd, Duration::from_secs(300));
    assert!(!server.status.success(), "served with no store");
    let stderr = String::from_utf8_lossy(&server.stderr);
    assert!(stderr.contains("LFS store is not reachable: list failed: backend error"), "{stderr}");
    assert!(!stderr.contains("127.0.0.1"), "{stderr}");
    assert!(!stderr.contains("AKIAUNREACHABLE0KEY"), "{stderr}");
    assert!(!stderr.contains("unreachable-secret-value"), "{stderr}");
}

#[test]
fn bad_repo_path_stops_the_server_and_stays_untouched() {
    let temp = TempDir::new("lb");
    let missing = temp.0.join("missing");
    let port = free_port().to_string();
    let server = run_server(&["--http-port", &port, missing.to_str().expect("utf-8 path")]);
    assert!(!server.status.success(), "served a path with no repo");
    assert!(!missing.exists(), "startup created {}", missing.display());
}

#[test]
fn convert_plain_repo_to_lfs_and_push() {
    let temp = TempDir::new("lcv");
    let cache = temp.0.join("cache");
    let versions: Vec<Vec<u8>> = [0u8, 1]
        .iter()
        .map(|seed| lfs_blob().iter().map(|byte| byte ^ seed).collect())
        .collect();

    assert_hg(&hg(&temp.0, &["init", "old"]), "hg init old");
    let old = temp.0.join("old");
    fs::create_dir(old.join("art")).expect("mkdir art");
    fs::write(old.join("README"), "hello\n").expect("write README");
    for (index, bytes) in versions.iter().enumerate() {
        fs::write(old.join("art").join("a.psd"), bytes).expect("write a.psd");
        let message = format!("rev {index}");
        assert_hg(&hg(&old, &["commit", "-Aqm", &message, "-u", "test"]), "hg commit");
    }

    let convert = [
        "--config",
        "extensions.convert=",
        "--config",
        "extensions.lfs=",
        "--config",
        "lfs.track=**.psd or size(\">10MB\")",
    ];
    assert_hg(&hg(&temp.0, &[&convert[..], &["convert", "old", "new"]].concat()), "hg convert");
    let new = temp.0.join("new");
    // convert leaves `new` at the null revision, so without this `.hglfs` would be a second root.
    assert_hg(&hg_lfs(&new, &cache, &["update"]), "hg update");
    fs::write(new.join(".hglfs"), "[track]\n**.psd = all()\n** = size(\">10MB\")\n")
        .expect("write .hglfs");
    let commit = ["commit", "-Aqm", "track large files", "-u", "test"];
    assert_hg(&hg_lfs(&new, &cache, &commit), "commit .hglfs");

    let served = init_empty_repo(&temp.0);
    let log = temp.0.join("mononoke.log");
    let server = Running::start(&served, &log);
    let url = format!("http://127.0.0.1:{}/", server.port);
    assert_ok_with_log(&hg_lfs(&new, &cache, &["push", &url]), "push converted", &log);

    let clone_cache = temp.0.join("clone-cache");
    assert_ok_with_log(&hg_lfs(&temp.0, &clone_cache, &["clone", &url, "clone"]), "clone", &log);
    let clone = temp.0.join("clone");
    for (index, bytes) in versions.iter().enumerate() {
        let rev = index.to_string();
        // To a file: `output_timeout` reads the pipes only after exit, and a blob fills one.
        let out = temp.0.join(format!("a{index}.psd"));
        let args = ["cat", "-r", &rev, "-o", &cfg_path(&out), "art/a.psd"];
        assert_ok_with_log(&hg_lfs(&clone, &clone_cache, &args), "cat", &log);
        assert!(fs::read(&out).expect("cat output") == *bytes, "art/a.psd differs at rev {index}");
    }
    let query = |args: &[&str]| {
        let out = hg_lfs(&clone, &clone_cache, args);
        assert_ok_with_log(&out, "query", &log);
        String::from_utf8_lossy(&out.stdout).into_owned()
    };
    assert_eq!(query(&["heads", "-T", "{rev}\n"]).lines().count(), 1, "heads");
    assert_eq!(query(&["log", "-r", "tip", "-T", "{desc}"]), "track large files");
    assert_eq!(query(&["log", "-r", "parents(tip)", "-T", "{desc}"]), "rev 1");
    let files = query(&["files", "-r", "tip", "-T", "{path}\n"]);
    assert!(files.lines().any(|f| f == ".hglfs") && files.lines().any(|f| f == "art/a.psd"), "{files}");
    let objects = served.join(".hg").join("store").join("lfs").join("objects");
    let blobs = fs::read_dir(objects)
        .expect("objects")
        .map(|entry| entry.expect("entry").path())
        .filter(|dir| dir.file_name().is_some_and(|name| name.len() == 2))
        .map(|dir| fs::read_dir(dir).expect("fanout").count())
        .sum::<usize>();
    assert_eq!(blobs, 2, "blobs in the served repository");
}
