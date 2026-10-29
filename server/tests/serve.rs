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
        let _guard = lock_ports();
        let port = free_port();
        let mut child = Command::new("hg")
            .current_dir(repo)
            .args(["serve", "-a", "127.0.0.1", "-p", &port.to_string()])
            .args(["--config", "web.push_ssl=false"])
            .args(["--config", "web.allow-push=*"])
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
        assert_ok_with_log(&hg(&client, &["push", &url]), "push", &log);
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
