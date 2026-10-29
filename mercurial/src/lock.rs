// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Mercurial's repository lock files, `.hg/wlock` and `.hg/store/lock`.
//!
//! `hg` takes these before it writes, so a writer that takes them too never
//! writes at the same time as `hg`. The file is created exclusively and holds
//! `host:pid`, the form Mercurial's `util.makelock` falls back to when it cannot
//! make a symlink, and which `util.readlock` reads on every platform.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// A held lock. Dropping it removes the file.
#[derive(Debug)]
pub struct RepoLock {
    path: PathBuf,
}

impl RepoLock {
    /// Take the lock at `path` if it is free. `Ok(None)` means someone holds it.
    pub fn try_acquire(path: &Path) -> io::Result<Option<RepoLock>> {
        let mut file = match OpenOptions::new().write(true).create_new(true).open(path) {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => return Ok(None),
            Err(err) => return Err(err),
        };
        let lock = RepoLock {
            path: path.to_path_buf(),
        };
        file.write_all(format!("{}:{}", hostname(), std::process::id()).as_bytes())?;
        Ok(Some(lock))
    }

    /// Who holds the lock at `path`, as Mercurial wrote it (`host:pid`), if anyone.
    pub fn holder(path: &Path) -> Option<String> {
        // Mercurial makes a symlink where it can, and a plain file otherwise.
        let raw = fs::read_link(path)
            .map(|target| target.to_string_lossy().into_owned())
            .or_else(|_| fs::read(path).map(|bytes| String::from_utf8_lossy(&bytes).into_owned()));
        raw.ok()
    }
}

impl Drop for RepoLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// The host name for the lock file. Mercurial compares it with its own to
/// decide whether it can tell that a lock's process has died.
fn hostname() -> String {
    std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .or_else(|_| {
            fs::read_to_string("/proc/sys/kernel/hostname").map(|name| name.trim().to_owned())
        })
        .unwrap_or_else(|_| "localhost".to_owned())
}

#[cfg(test)]
mod test {
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("mononoke-lock-{label}-{nanos}"));
        fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    #[test]
    fn lock_is_exclusive_and_removed_on_drop() {
        let dir = temp_dir("x");
        let path = dir.join("lock");
        let lock = RepoLock::try_acquire(&path)
            .expect("acquire")
            .expect("free");
        let holder = RepoLock::holder(&path).expect("holder");
        assert!(
            holder.ends_with(&format!(":{}", std::process::id())),
            "{holder}"
        );
        assert!(RepoLock::try_acquire(&path).expect("second").is_none());
        drop(lock);
        assert!(!path.exists());
        assert!(RepoLock::holder(&path).is_none());
        RepoLock::try_acquire(&path)
            .expect("again")
            .expect("free again");
        fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn hg_sees_the_lock_as_held_by_this_process() {
        let dir = temp_dir("hg");
        let init = Command::new("hg")
            .current_dir(&dir)
            .args(["init", "repo"])
            .env("HGPLAIN", "1")
            .env("HGRCPATH", "")
            .output()
            .expect("spawn hg init");
        assert!(init.status.success(), "hg init: {init:?}");
        let repo = dir.join("repo");
        let lock = RepoLock::try_acquire(&repo.join(".hg").join("store").join("lock"))
            .expect("acquire")
            .expect("free");

        // `hg debuglocks` exits with the number of held locks, so read its output.
        let out = Command::new("hg")
            .current_dir(&repo)
            .args(["debuglocks"])
            .env("HGPLAIN", "1")
            .env("HGRCPATH", "")
            .output()
            .expect("spawn hg debuglocks");
        let text = String::from_utf8_lossy(&out.stdout);
        let lock_line = text
            .lines()
            .find(|line| line.starts_with("lock:"))
            .unwrap_or_else(|| panic!("no lock line:\n{text}"));
        assert!(
            lock_line.contains(&format!("process {}", std::process::id())),
            "{text}"
        );
        drop(lock);
        fs::remove_dir_all(&dir).expect("cleanup");
    }
}
