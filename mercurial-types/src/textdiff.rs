// Copyright 2005, 2006 Olivia Mackall <olivia@selenic.com>
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.
//
// Ported from Mercurial 7.0.3 mercurial/bdiff.c and mercurial/cext/bdiff.c

//! Mercurial's line diff (`mdiff.textdiff`, the C `bdiff.bdiff`), ported so
//! deltas we generate are byte-identical to the ones `hg` generates.

use crate::delta::{Delta, Fragment};

struct Line {
    hash: u32,
    // Next line in b with the same content (b), or head of that chain (a).
    n: isize,
    // Equivalence class (hash table bucket).
    e: usize,
    start: usize,
    len: usize,
}

#[derive(Clone, Copy)]
struct Pos {
    pos: isize,
    len: isize,
}

struct Hunk {
    a1: usize,
    a2: usize,
    b1: usize,
    b2: usize,
}

/// `bdiff_splitlines`: one entry per line (the last may lack a newline) plus
/// a sentinel at the end of the text.
fn splitlines(text: &[u8]) -> Vec<Line> {
    let mut lines = Vec::new();
    let mut hash: u32 = 0;
    let mut b = 0;
    for (p, &c) in text.iter().enumerate() {
        // C hashes a (signed) `char`, so bytes >= 0x80 are sign-extended.
        hash = (c as i8 as i32 as u32).wrapping_add(hash.rotate_left(7));
        if c == b'\n' || p == text.len() - 1 {
            lines.push(Line {
                hash,
                n: isize::MAX,
                e: 0,
                start: b,
                len: p - b + 1,
            });
            hash = 0;
            b = p + 1;
        }
    }
    lines.push(Line {
        hash: 0,
        n: 0,
        e: 0,
        start: text.len(),
        len: 0,
    });
    lines
}

fn same(at: &[u8], a: &Line, bt: &[u8], b: &Line) -> bool {
    a.hash == b.hash && at[a.start..a.start + a.len] == bt[b.start..b.start + b.len]
}

fn equatelines(at: &[u8], a: &mut [Line], an: usize, bt: &[u8], b: &mut [Line], bn: usize) {
    let mut buckets = 1;
    while buckets < bn + 1 {
        buckets *= 2;
    }
    // C tries scale 4 first and only shrinks if calloc fails.
    let mask = buckets * 4 - 1;
    let mut h = vec![Pos { pos: -1, len: 0 }; mask + 1];

    for i in 0..bn {
        let mut j = b[i].hash as usize & mask;
        while h[j].pos != -1 {
            if same(bt, &b[i], bt, &b[h[j].pos as usize]) {
                break;
            }
            j = (j + 1) & mask;
        }
        b[i].n = h[j].pos;
        b[i].e = j;
        h[j].pos = i as isize;
        h[j].len += 1;
    }

    let t = if bn >= 31000 {
        bn / 1000
    } else {
        1000000 / (bn + 1)
    } as isize;

    for i in 0..an {
        let mut j = a[i].hash as usize & mask;
        while h[j].pos != -1 {
            if same(at, &a[i], bt, &b[h[j].pos as usize]) {
                break;
            }
            j = (j + 1) & mask;
        }
        a[i].e = j;
        a[i].n = if h[j].len <= t { h[j].pos } else { -1 };
    }
}

fn longest_match(
    a: &[Line],
    b: &[Line],
    pos: &mut [Pos],
    a1: isize,
    a2: isize,
    b1: isize,
    b2: isize,
) -> (isize, isize, isize) {
    let (mut mi, mut mj, mut mk) = (a1, b1, 0);
    let mut a1 = a1;

    if a2 - a1 > 30000 {
        a1 = a2 - 30000;
    }

    let half = (a1 + a2 - 1) / 2;
    let bhalf = (b1 + b2 - 1) / 2;

    for i in a1..a2 {
        let mut j = a[i as usize].n;
        while j >= b2 {
            j = b[j as usize].n;
        }

        while j >= b1 {
            let mut k = 1;
            while j - k >= b1 && i - k >= a1 {
                let p = pos[(j - k) as usize];
                if p.pos == i - k {
                    k += p.len;
                    break;
                }
                if a[(i - k) as usize].e != b[(j - k) as usize].e {
                    break;
                }
                k += 1;
            }

            pos[j as usize] = Pos { pos: i, len: k };

            if k > mk {
                mi = i;
                mj = j;
                mk = k;
            } else if k == mk {
                if i > mi && i <= half && j > b1 {
                    mi = i;
                    mj = j;
                } else if i == mi && (mj > bhalf || i == a1) {
                    mj = j;
                }
            }
            j = b[j as usize].n;
        }
    }

    if mk != 0 {
        mi = mi - mk + 1;
        mj = mj - mk + 1;
    }

    while mi + mk < a2 && mj + mk < b2 && a[(mi + mk) as usize].e == b[(mj + mk) as usize].e {
        mk += 1;
    }

    (mi, mj, mk)
}

/// `recurse`, with an explicit stack so deep left recursion can't overflow.
/// `longest_match` runs on the ranges in the same order as in C, which
/// matters because `pos` carries state between calls.
fn recurse(a: &[Line], b: &[Line], pos: &mut [Pos], an: usize, bn: usize, out: &mut Vec<Hunk>) {
    enum Task {
        Range(isize, isize, isize, isize),
        Emit(isize, isize, isize),
    }
    let mut stack = vec![Task::Range(0, an as isize, 0, bn as isize)];
    while let Some(task) = stack.pop() {
        match task {
            Task::Range(a1, a2, b1, b2) => {
                let (i, j, k) = longest_match(a, b, pos, a1, a2, b1, b2);
                if k == 0 {
                    continue;
                }
                stack.push(Task::Range(i + k, a2, j + k, b2));
                stack.push(Task::Emit(i, j, k));
                stack.push(Task::Range(a1, i, b1, j));
            }
            Task::Emit(i, j, k) => out.push(Hunk {
                a1: i as usize,
                a2: (i + k) as usize,
                b1: j as usize,
                b2: (j + k) as usize,
            }),
        }
    }
}

/// `bdiff_diff`: matching blocks, ending with an (an, an, bn, bn) sentinel.
fn diff(at: &[u8], a: &mut [Line], bt: &[u8], b: &mut [Line]) -> Vec<Hunk> {
    let (an, bn) = (a.len() - 1, b.len() - 1);
    equatelines(at, a, an, bt, b, bn);
    let mut pos = vec![Pos { pos: 0, len: 0 }; bn.max(1)];
    let mut hunks = Vec::new();
    recurse(a, b, &mut pos, an, bn, &mut hunks);
    hunks.push(Hunk {
        a1: an,
        a2: an,
        b1: bn,
        b2: bn,
    });

    // Normalize the hunk list, pushing each hunk towards the end.
    for x in 0..hunks.len() - 1 {
        let (cur, next) = hunks.split_at_mut(x + 1);
        let (cur, next) = (&mut cur[x], &mut next[0]);
        if cur.a2 == next.a1 || cur.b2 == next.b1 {
            while cur.a2 < an
                && cur.b2 < bn
                && next.a1 < next.a2
                && next.b1 < next.b2
                && same(at, &a[cur.a2], bt, &b[cur.b2])
            {
                cur.a2 += 1;
                next.a1 += 1;
                cur.b2 += 1;
                next.b1 += 1;
            }
        }
    }
    hunks
}

/// The delta `mdiff.textdiff(a, b)` (C `bdiff.bdiff`) produces.
pub fn textdiff(a: &[u8], b: &[u8]) -> Delta {
    // Skip the common prefix of whole lines.
    let mut lcommon = 0;
    for (li, (x, y)) in a.iter().zip(b).enumerate() {
        if x != y {
            break;
        }
        if *x == b'\n' {
            lcommon = li + 1;
        }
    }
    let (at, bt) = (&a[lcommon..], &b[lcommon..]);
    let mut al = splitlines(at);
    let mut bl = splitlines(bt);
    let hunks = diff(at, &mut al, bt, &mut bl);

    let mut frags = Vec::new();
    let (mut la, mut lb) = (0, 0);
    for h in hunks {
        if h.a1 != la || h.b1 != lb {
            frags.push(Fragment {
                start: al[la].start + lcommon,
                end: al[h.a1].start + lcommon,
                content: bt[bl[lb].start..bl[h.b1].start].to_vec(),
            });
        }
        la = h.a2;
        lb = h.b2;
    }
    Delta::new(frags).expect("bdiff hunks are ordered")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta;
    use std::process::Command;

    fn pack(d: &Delta) -> Vec<u8> {
        let mut out = Vec::new();
        for f in d.fragments() {
            out.extend_from_slice(&(f.start as u32).to_be_bytes());
            out.extend_from_slice(&(f.end as u32).to_be_bytes());
            out.extend_from_slice(&(f.content.len() as u32).to_be_bytes());
            out.extend_from_slice(&f.content);
        }
        out
    }

    fn hg_textdiff(a: &[u8], b: &[u8]) -> Vec<u8> {
        let dir = std::env::temp_dir().join(format!("textdiff-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (pa, pb) = (dir.join("a"), dir.join("b"));
        std::fs::write(&pa, a).unwrap();
        std::fs::write(&pb, b).unwrap();
        let script = format!(
            "import sys; from mercurial import mdiff; _ = sys.stdout.buffer.write(mdiff.textdiff(open(r'{}','rb').read(), open(r'{}','rb').read()))",
            pa.display(),
            pb.display()
        );
        let out = Command::new("hg")
            .args(["debugshell", "-c", &script])
            .env("HGPLAIN", "1")
            .env("HGRCPATH", "")
            .output()
            .expect("`hg` must be on PATH to compare against Mercurial's bdiff");
        assert!(
            out.status.success(),
            "hg debugshell failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        out.stdout
    }

    #[test]
    fn matches_mercurial_on_samples() {
        let big: Vec<u8> = (0..120_000)
            .flat_map(|i| format!("line number {}\n", i % 5000).into_bytes())
            .collect();
        assert!(big.len() > 2_000_000);
        let mut big_edited = big.clone();
        for i in (1000..big_edited.len()).step_by(99_991) {
            big_edited[i] = b'#';
            big_edited.insert(i + 7, b'\n');
        }
        let many_a = b"a\n".repeat(1000);
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (b"".to_vec(), b"x\ny\n".to_vec()),
            (b"x\ny\n".to_vec(), b"".to_vec()),
            (b"x\ny\nz\n".to_vec(), b"x\ny\nz\n".to_vec()),
            (b"a\nb\nc\nd\n".to_vec(), b"a\nb\nX\nd\n".to_vec()),
            (b"a\nb\nc\n".to_vec(), b"new\na\nb\nc\n".to_vec()),
            (b"a\nb\nc\n".to_vec(), b"a\nnew\nmore\nb\nc\n".to_vec()),
            (b"a\nb\nc\n".to_vec(), b"a\nb\nc\nnew\n".to_vec()),
            (b"a\nb\nc".to_vec(), b"a\nb\nd".to_vec()),
            (b"a\nb\nc\n".to_vec(), b"a\nb\nc".to_vec()),
            (b"a\r\nb\r\nc\r\n".to_vec(), b"a\r\nB\r\nc\r\n".to_vec()),
            (big, big_edited),
            (many_a.clone(), many_a[2..].to_vec()),
            (many_a.clone(), [&many_a[..1000], &many_a[1002..]].concat()),
            (
                vec![0u8, 1, 2, 0xff, 0x80, 7, 9],
                vec![0u8, 1, 3, 0xff, 0x80, 7, 9, 10],
            ),
        ];
        for (a, b) in &pairs {
            assert_eq!(
                pack(&textdiff(a, b)),
                hg_textdiff(a, b),
                "a={:?} b={:?}",
                String::from_utf8_lossy(&a[..a.len().min(80)]),
                String::from_utf8_lossy(&b[..b.len().min(80)])
            );
        }
    }

    quickcheck::quickcheck! {
        fn applies_back(a: Vec<u8>, b: Vec<u8>) -> bool {
            // A small alphabet with newlines gives lines that repeat and match.
            let a: Vec<u8> = a.iter().map(|x| b"ab\n"[*x as usize % 3]).collect();
            let b: Vec<u8> = b.iter().map(|x| b"ab\n"[*x as usize % 3]).collect();
            delta::apply(&a, textdiff(&a, &b)) == b
        }
    }
}
