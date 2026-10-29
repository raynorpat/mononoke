//! Named branches and Mercurial's history-aware tag merge.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::path::Path;

use mercurial::changeset::RevlogChangeset;
use mercurial::revlog::RevIdx;
use mercurial::RevlogRepo;
use mercurial_types::{Changeset, NodeHash, NULL_HASH};

use crate::errors::Result;

// Mercurial's encoding.tolocal falls back to Latin-1 for old names that
// predate its UTF-8 convention; fromlocal sends those names as UTF-8.
fn name_utf8(name: &[u8]) -> Vec<u8> {
    match std::str::from_utf8(name) {
        Ok(_) => name.to_vec(),
        Err(_) => name
            .iter()
            .map(|&b| char::from(b))
            .collect::<String>()
            .into_bytes(),
    }
}

#[derive(Default)]
pub struct BranchCache {
    names: Vec<Vec<u8>>,
    branch_of: Vec<u32>,
    closed: Vec<bool>,
}

impl BranchCache {
    pub fn extend(&mut self, repo: &RevlogRepo) -> Result<()> {
        let log = repo.changelog_revlog();
        for rev in self.branch_of.len() as u32..log.len()? {
            let cs = RevlogChangeset::new(log.get_rev(RevIdx::from(rev))?)?;
            let name = name_utf8(
                cs.extra()
                    .get(b"branch".as_slice())
                    .map(Vec::as_slice)
                    .unwrap_or(b"default"),
            );
            let branch = match self.names.iter().position(|n| n == &name) {
                Some(i) => i,
                None => {
                    self.names.push(name);
                    self.names.len() - 1
                }
            };
            self.branch_of.push(branch as u32);
            self.closed
                .push(cs.extra().contains_key(b"close".as_slice()));
        }
        Ok(())
    }

    pub fn heads(&self, parents: &[[Option<u32>; 2]]) -> BTreeMap<Vec<u8>, Vec<u32>> {
        let mut branches: Vec<Vec<u32>> = vec![Vec::new(); self.names.len()];
        for (rev, &branch) in self.branch_of.iter().enumerate() {
            branches[branch as usize].push(rev as u32);
        }
        let mut result = BTreeMap::new();
        for (branch, revisions) in branches.into_iter().enumerate() {
            let mut heads = BTreeSet::new();
            let mut uncertain = Vec::new();
            for rev in revisions {
                let mut same = Vec::new();
                let mut other = Vec::new();
                for p in parents[rev as usize].into_iter().flatten() {
                    if self.branch_of[p as usize] == branch as u32 {
                        same.push(p);
                    } else {
                        other.push(p);
                    }
                }
                // A different-branch parent can carry an older head of this
                // branch. Mercurial defers those ancestry checks until the end.
                if !(heads.len() == 1 && same.len() == 1) {
                    uncertain.extend(other);
                }
                for p in same {
                    heads.remove(&p);
                }
                heads.insert(rev);
            }
            if let Some(&floor) = heads.first() {
                let mut seen = HashSet::new();
                while let Some(rev) = uncertain.pop() {
                    if rev < floor || !seen.insert(rev) {
                        continue;
                    }
                    heads.remove(&rev);
                    uncertain.extend(parents[rev as usize].into_iter().flatten());
                }
            }
            result.insert(self.names[branch].clone(), heads.into_iter().collect());
        }
        result
    }

    pub fn tip(&self, name: &[u8], parents: &[[Option<u32>; 2]]) -> Option<u32> {
        let heads = self.heads(parents);
        let heads = heads.get(name)?;
        heads
            .iter()
            .rev()
            .find(|&&r| !self.closed[r as usize])
            .or_else(|| heads.last())
            .copied()
    }
}

pub fn changelog_parents(repo: &RevlogRepo) -> Result<Vec<[Option<u32>; 2]>> {
    let log = repo.changelog_revlog();
    (0..log.len()?)
        .map(|r| {
            let e = log.get_entry(RevIdx::from(r))?;
            Ok([e.p1.map(RevIdx::as_u32), e.p2.map(RevIdx::as_u32)])
        })
        .collect()
}

type Tags = BTreeMap<Vec<u8>, (NodeHash, Vec<NodeHash>)>;

fn read_tags(data: &[u8]) -> Tags {
    let mut tags = Tags::new();
    for line in data.split(|b| *b == b'\n' || *b == b'\r') {
        let Some(space) = line.iter().position(|b| *b == b' ') else {
            continue;
        };
        let Ok(hex) = std::str::from_utf8(&line[..space]) else {
            continue;
        };
        let Ok(node) = hex.parse::<NodeHash>() else {
            continue;
        };
        let name = name_utf8(line[space + 1..].trim_ascii());
        if let Some((last, history)) = tags.get_mut(&name) {
            history.push(*last);
            *last = node;
        } else {
            tags.insert(name, (node, Vec::new()));
        }
    }
    tags
}

fn update_tags(file: Tags, tags: &mut Tags) {
    for (name, (mut node, mut history)) in file {
        if let Some((old, previous)) = tags.get(&name) {
            if *old != node
                && previous.contains(&node)
                && (!history.contains(old) || previous.len() > history.len())
            {
                node = *old;
            }
            // Membership is checked against the incoming history before it
            // is extended: repeated old nodes still contribute to its rank.
            let inherited: Vec<_> = previous
                .iter()
                .filter(|n| !history.contains(n))
                .copied()
                .collect();
            history.extend(inherited);
        }
        tags.insert(name, (node, history));
    }
}

pub fn global_tags(repo: &RevlogRepo, heads: &[NodeHash]) -> Result<Vec<(Vec<u8>, NodeHash)>> {
    let log = repo.changelog_revlog();
    let mut revisions = heads
        .iter()
        .filter(|n| **n != NULL_HASH)
        .map(|n| log.index_of(n))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    revisions.sort_unstable();
    let path = mercurial_types::Path::new(b".hgtags").map_err(mercurial::Error::from)?;
    let mut seen = HashSet::new();
    let mut tags = Tags::new();
    for rev in revisions {
        let cs = RevlogChangeset::new(log.get_rev(RevIdx::from(rev))?)?;
        if *cs.manifestid() == NULL_HASH {
            continue;
        }
        let manifest = repo.manifest_revlog().get_rev_by_nodeid(cs.manifestid())?;
        let manifest = mercurial::RevlogManifest::new(repo.clone(), manifest)?;
        let Some(entry) = manifest.lookup(&path) else {
            continue;
        };
        if !seen.insert(*entry.nodeid()) {
            continue;
        }
        let blob = repo
            .read_file_revlog(&path)?
            .get_rev_by_nodeid(entry.nodeid())?;
        let raw = blob.as_blob().as_slice().ok_or("missing tag file data")?;
        if raw.len() < 2 {
            continue;
        }
        let file = mercurial::file::File::new(blob);
        update_tags(
            read_tags(file.content().ok_or("missing tag file data")?),
            &mut tags,
        );
    }
    match fs::read(
        repo.store_path()
            .parent()
            .expect("store under hg")
            .join("localtags"),
    ) {
        Ok(data) => {
            let local = read_tags(&data)
                .into_iter()
                .filter(|(_, (node, _))| *node == NULL_HASH || log.index_of(node).is_ok())
                .collect();
            update_tags(local, &mut tags);
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    Ok(tags
        .into_iter()
        .filter_map(|(name, (node, _))| (node != NULL_HASH).then_some((name, node)))
        .collect())
}

pub fn dirstate_parent(hg_dir: &Path) -> Result<NodeHash> {
    let data = match fs::read(hg_dir.join("dirstate")) {
        Ok(data) => data,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(NULL_HASH),
        Err(e) => return Err(e.into()),
    };
    if data.is_empty() {
        return Ok(NULL_HASH);
    }
    let offset = if data.starts_with(b"dirstate-v2\n") {
        if data.len() < 125 {
            return Err("truncated dirstate-v2 docket".into());
        }
        12
    } else {
        0
    };
    let parent = data
        .get(offset..offset + 20)
        .ok_or("truncated dirstate parent")?;
    NodeHash::from_bytes(parent).map_err(|e| e.to_string().into())
}

pub fn resolve_key(
    repo: &RevlogRepo,
    branches: &BranchCache,
    bookmarks: &[(Vec<u8>, Vec<u8>)],
    key: &[u8],
) -> std::result::Result<NodeHash, String> {
    let log = repo.changelog_revlog();
    let len = i64::from(log.len().map_err(|e| e.to_string())?);
    let node_at = |r: u32| {
        log.get_entry(RevIdx::from(r))
            .map(|e| e.nodeid)
            .map_err(|e| e.to_string())
    };
    match key {
        b"." => {
            return dirstate_parent(repo.store_path().parent().expect("store under hg"))
                .map_err(|e| e.to_string())
        }
        b"null" => return Ok(NULL_HASH),
        b"tip" => {
            return if len == 0 {
                Ok(NULL_HASH)
            } else {
                node_at((len - 1) as u32)
            }
        }
        _ => {}
    }
    let ascii = std::str::from_utf8(key).ok().filter(|s| s.is_ascii());
    if let Some(text) = ascii {
        if let Ok(num) = text.parse::<i64>() {
            let rev = if num < 0 { num + len } else { num };
            if num.to_string() == text && (0..len).contains(&rev) {
                return node_at(rev as u32);
            }
        }
        if text.len() == 40 {
            if let Ok(node) = text.parse::<NodeHash>() {
                if node == NULL_HASH || log.index_of(&node).is_ok() {
                    return Ok(node);
                }
            }
        }
    }
    let name = name_utf8(key);
    let mut named_node = bookmarks
        .iter()
        .find(|(n, _)| n == &name)
        .and_then(|(_, hex)| std::str::from_utf8(hex).ok()?.parse().ok());
    if named_node.is_none() {
        let heads: Vec<_> = repo
            .changelog_revlog()
            .get_heads()
            .map_err(|e| e.to_string())?
            .into_iter()
            .collect();
        named_node = global_tags(repo, &heads)
            .map_err(|e| e.to_string())?
            .into_iter()
            .find(|(n, _)| n == &name)
            .map(|(_, node)| node);
        if named_node.is_none() {
            if let Some(rev) =
                branches.tip(&name, &changelog_parents(repo).map_err(|e| e.to_string())?)
            {
                named_node = Some(node_at(rev)?);
            }
        }
    }
    // revsymbol validates the winning namespace's node before returning it.
    // A missing target skips the other namespaces and still tries a hash prefix.
    if let Some(node) = named_node {
        if node == NULL_HASH || log.index_of(&node).is_ok() {
            return Ok(node);
        }
    }
    if let Some(text) = ascii {
        let prefix = text.strip_prefix('x').unwrap_or(text);
        if !prefix.is_empty() && prefix.len() <= 40 && prefix.bytes().all(|b| b.is_ascii_hexdigit())
        {
            let prefix = prefix.to_ascii_lowercase();
            let mut found = None;
            let mut count = 0;
            for rev in 0..len {
                let node = node_at(rev as u32)?;
                if node.to_hex().as_str().starts_with(&prefix) {
                    found = Some(node);
                    count += 1;
                }
            }
            if "0".repeat(40).starts_with(&prefix) {
                found = Some(NULL_HASH);
                count += 1;
            }
            if "f".repeat(40).starts_with(&prefix) {
                count += 1;
            }
            if count > 1 {
                return Err(format!("00changelog@{prefix}: ambiguous identifier"));
            }
            if let Some(node) = found {
                return Ok(node);
            }
        }
    }
    Err(format!(
        "unknown revision '{}'",
        String::from_utf8_lossy(key)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct Fixture(PathBuf);
    impl Fixture {
        fn new(label: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "mononoke-names-{label}-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            fs::create_dir_all(&dir).unwrap();
            hg(
                &dir,
                &["init", "--config", "format.use-persistent-nodemap=false"],
            );
            fs::write(dir.join("a"), "base").unwrap();
            hg(&dir, &["add", "a"]);
            hg(&dir, &["commit", "-m", "base", "-u", "test"]);
            Self(dir)
        }
        fn commit(&self, text: &str) {
            fs::write(self.0.join("a"), text).unwrap();
            hg(&self.0, &["commit", "-m", text, "-u", "test"]);
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn hg(dir: &Path, args: &[&str]) -> Vec<u8> {
        let out = Command::new("hg")
            .current_dir(dir)
            .args(args)
            .args(["--config", "storage.dirstate-v2.slow-path=allow"])
            .env("HGPLAIN", "1")
            .env("HGRCPATH", "")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "hg {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        out.stdout
    }
    fn parents(repo: &mercurial::RevlogRepo) -> Vec<[Option<u32>; 2]> {
        let log = repo.changelog_revlog();
        (0..log.len().unwrap())
            .map(|r| {
                let e = log.get_entry(mercurial::revlog::RevIdx::from(r)).unwrap();
                [e.p1.map(|p| p.as_u32()), e.p2.map(|p| p.as_u32())]
            })
            .collect()
    }

    #[test]
    fn branch_heads_follow_mercurial() {
        let f = Fixture::new("branches");
        hg(&f.0, &["branch", "stable"]);
        f.commit("stable");
        hg(
            &f.0,
            &["commit", "--close-branch", "-m", "close", "-u", "test"],
        );
        hg(&f.0, &["update", "-r", "1"]);
        f.commit("open");
        hg(&f.0, &["update", "-r", "0"]);
        hg(&f.0, &["branch", "feature x"]);
        f.commit("feature");
        let repo = mercurial::RevlogRepo::open(f.0.join(".hg")).unwrap();
        let mut cache = BranchCache::default();
        cache.extend(&repo).unwrap();
        let actual: BTreeSet<_> = cache
            .heads(&parents(&repo))
            .into_iter()
            .flat_map(|(n, rs)| {
                rs.into_iter()
                    .map(move |r| format!("{}:{r}", String::from_utf8_lossy(&n)))
            })
            .collect();
        let out = hg(&f.0, &["heads", "--closed", "-T", "{branch}:{rev}\n"]);
        let expected = String::from_utf8(out)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect();
        assert_eq!(actual, expected);
        assert_eq!(cache.tip(b"stable", &parents(&repo)), Some(3));
    }

    #[test]
    fn branch_heads_follow_transitions() {
        let f = Fixture::new("transitions");
        hg(&f.0, &["branch", "stable"]);
        f.commit("stable");
        hg(&f.0, &["branch", "-f", "default"]);
        f.commit("back");
        let repo = mercurial::RevlogRepo::open(f.0.join(".hg")).unwrap();
        let mut cache = BranchCache::default();
        cache.extend(&repo).unwrap();
        let heads = cache.heads(&parents(&repo));
        assert_eq!(heads[b"default".as_slice()], vec![2]);
        assert_eq!(heads[b"stable".as_slice()], vec![1]);
    }

    #[test]
    fn global_tags_follow_mercurial() {
        let f = Fixture::new("tags");
        hg(&f.0, &["tag", "v1.0", "-u", "test"]);
        hg(&f.0, &["tag", "moved", "-r", "0", "-u", "test"]);
        hg(&f.0, &["tag", "-f", "moved", "-r", "1", "-u", "test"]);
        hg(&f.0, &["tag", "removed", "-r", "0", "-u", "test"]);
        hg(&f.0, &["tag", "--remove", "removed", "-u", "test"]);
        hg(&f.0, &["update", "-r", "1"]);
        hg(&f.0, &["tag", "-f", "other-head", "-r", "0", "-u", "test"]);
        // A newer head's stale assignment must not undo the move from the older head.
        hg(&f.0, &["tag", "--local", "local", "-r", "0"]);
        let node0 = hg(&f.0, &["log", "-r", "0", "-T", "{node}"]);
        let mut local = fs::read(f.0.join(".hg/localtags")).unwrap();
        local.extend_from_slice(&node0);
        local.extend_from_slice(
            b" moved\nfffffffffffffffffffffffffffffffffffffffe invalid\nmalformed\n",
        );
        fs::write(f.0.join(".hg/localtags"), local).unwrap();
        let repo = mercurial::RevlogRepo::open(f.0.join(".hg")).unwrap();
        let heads: Vec<_> = repo
            .changelog_revlog()
            .get_heads()
            .unwrap()
            .into_iter()
            .collect();
        let got: BTreeMap<_, _> = global_tags(&repo, &heads).unwrap().into_iter().collect();
        let out = hg(&f.0, &["tags", "-T", "{tag}:{node}\n"]);
        let expected: BTreeMap<_, _> = out
            .split(|b| *b == b'\n')
            .filter(|l| !l.is_empty())
            .filter_map(|line| {
                let i = line.iter().position(|b| *b == b':').unwrap();
                (line[..i] != *b"tip").then(|| {
                    (
                        line[..i].to_vec(),
                        std::str::from_utf8(&line[i + 1..])
                            .unwrap()
                            .parse()
                            .unwrap(),
                    )
                })
            })
            .collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn tag_history_duplicates_keep_their_rank() {
        let a = NodeHash::from_bytes(&[1; 20]).unwrap();
        let b = NodeHash::from_bytes(&[2; 20]).unwrap();
        let name = b"ranked".to_vec();
        let mut tags = BTreeMap::from([(name.clone(), (b, vec![a, a, a]))]);
        update_tags(BTreeMap::from([(name.clone(), (a, vec![]))]), &mut tags);
        update_tags(BTreeMap::from([(name.clone(), (a, vec![b, b]))]), &mut tags);
        // The existing history's rank is three, so it outranks the new rank two.
        assert_eq!(tags[&name].0, b);
    }

    #[test]
    fn dirstate_v2_parent_matches_mercurial() {
        let f = Fixture::new("dirstate");
        hg(
            &f.0,
            &[
                "debugupgraderepo",
                "--run",
                "--config",
                "format.use-dirstate-v2=true",
            ],
        );
        let expected = hg(&f.0, &["log", "-r", ".", "-T", "{node}"]);
        assert_eq!(
            dirstate_parent(&f.0.join(".hg"))
                .unwrap()
                .to_hex()
                .as_str()
                .as_bytes(),
            expected
        );
        assert!(fs::read(f.0.join(".hg/dirstate"))
            .unwrap()
            .starts_with(b"dirstate-v2\n"));
    }
}
