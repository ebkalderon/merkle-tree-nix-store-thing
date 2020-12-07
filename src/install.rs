//! Internal method for converting external directories into `Package` objects.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{anyhow, Context};

use crate::local::Packages;
use crate::reference::{ReferenceSink, RewriteSink};
use crate::{
    Backend, Blob, Entry, HashWriter, Hasher, Object, ObjectId, Package, Platform, Spec, Store,
    Tree,
};

const PATCHELF_BIN: &str = "patchelf";

impl<B: Backend> Store<B> {
    /// Converts an external directory into a [`Package`] object and installs it in the store.
    ///
    /// Blobs found to contain references to their own install directory ("self-references") are
    /// inserted into the store with the hash component of every occurrence normalized to
    /// [`ObjectId::zero()`]. These zeroed hashes are later patched out to the real package hash by
    /// a call to [`rewrite_paths()`] at istall time. Unfortunately, this in-place patching means
    /// these particular blob files must be copied over from the `objects` directory instead of
    /// hard-linked, meaning they have some inherent storage overhead.
    ///
    /// This method tries to never modify the original contents of `out_dir` during this process,
    /// with one notable exception: executable files found to contain RPATH self-references will be
    /// patched _in-place_ before they are further processed and inserted into the store.
    ///
    /// Returns the ID of the installed package object.
    pub(crate) fn install_path(&mut self, out_dir: &Path, spec: &Spec) -> anyhow::Result<ObjectId> {
        debug_assert!(out_dir.is_dir());
        debug_assert!(out_dir.is_absolute());

        let name = format!("{}-{}", spec.name, spec.version).parse()?;
        let (tree_id, references, self_refs) = build_tree(self, out_dir, out_dir, spec)?;

        if !references.is_subset(&spec.dependencies) {
            return Err(anyhow!(
                "{:?} points to outside dependencies: {:?}",
                name,
                references
            ));
        }

        self.insert_object(Object::Package(Package {
            name,
            system: spec.target.unwrap_or_else(|| Platform::host()),
            references,
            self_references: self_refs,
            tree: tree_id,
        }))
    }
}

/// Rewrites every zeroed self-reference inside `writer` with the final path string `new`.
///
/// To avoid searching through the entire file, this function jumps to every position in `offsets`
/// and overwrites the data with `new`. It blindly trusts that the values in `offsets` are correct,
/// so please use with care.
///
/// This function is intended to be called at package install time.
pub fn rewrite_paths<W>(writer: &mut W, new: &Path, offsets: &BTreeSet<u64>) -> anyhow::Result<()>
where
    W: Write + Seek,
{
    let final_path = new.to_str().ok_or(anyhow!("new path is invalid UTF-8"))?;

    for &offset in offsets {
        writer
            .seek(SeekFrom::Start(offset))
            .with_context(|| format!("failed to seek to offset {}", offset))?;
        writer
            .write_all(final_path.as_bytes())
            .with_context(|| format!("failed to rewrite offset {} with new path", offset))?;
    }

    writer.flush()?;

    Ok(())
}

/// Recursively inserts the contents of `tree_dir` in the store as a tree object, patching out any
/// self-references to `out_dir` detected in blobs and symlinks by converting them to relative
/// paths. This is to maintain the content addressable invariant of the store.
///
/// Returns the ID of the installed tree object, the detected run-time references, and a set of
/// blob objects that contain self-references.
fn build_tree<B>(
    store: &mut Store<B>,
    tree_dir: &Path,
    out_dir: &Path,
    spec: &Spec,
) -> anyhow::Result<(
    ObjectId,
    BTreeSet<ObjectId>,
    BTreeMap<ObjectId, BTreeSet<u64>>,
)>
where
    B: Backend,
{
    debug_assert!(tree_dir.starts_with(out_dir));

    let mut references = BTreeSet::new();
    let mut self_references = BTreeMap::new();
    let mut entries = BTreeMap::new();

    let iter = std::fs::read_dir(tree_dir)?;
    let mut children: Vec<_> = iter.collect::<Result<_, _>>()?;
    children.sort_by_cached_key(|entry| entry.path());

    for child in children {
        let path = child.path();
        let file_name = path
            .file_name()
            .expect("path must have filename")
            .to_str()
            .ok_or_else(|| anyhow!("path {} contains invalid UTF-8", path.display()))?
            .to_owned();

        let file_type = child.file_type()?;
        if file_type.is_dir() {
            let (id, refs, self_refs) = build_tree(store, &path, out_dir, spec)?;
            references.extend(refs);
            self_references.extend(self_refs);
            entries.insert(file_name, Entry::Tree { id });
        } else if file_type.is_file() {
            let pkgs_dir = store.packages.path();
            let (blob, refs, offsets) = make_content_addressed(&path, out_dir, &pkgs_dir, spec)?;

            let id = store.insert_object(Object::Blob(blob))?;
            references.extend(refs);
            if !offsets.is_empty() {
                self_references.insert(id, offsets);
            }

            entries.insert(file_name, Entry::Blob { id });
        } else if file_type.is_symlink() {
            let target = path.read_link()?;
            let norm_target = target.canonicalize()?;

            let target = if norm_target.starts_with(out_dir) {
                pathdiff::diff_paths(norm_target, out_dir).expect("both paths are absolute")
            } else {
                target
            };

            entries.insert(file_name, Entry::Symlink { target });
        } else {
            unreachable!("entries can only be files, directories, or symlinks");
        }
    }

    let tree_id = store.insert_object(Object::Tree(Tree { entries }))?;

    Ok((tree_id, references, self_references))
}

/// Prepares `file` for insertion into the store as a blob object.
///
/// This function scans the contents of `file` for run-time references, replacing any detected
/// self-references to `out_dir` as a fixed value (in this case, the final install directory but
/// with the cryptographic hash component set to [`ObjectId::zero()`]).
///
/// The original contents of `file` are not modified during this process, as temp files are used.
/// However, if `file` is an executable with self-references, its RPATHs will be patched _in-place_
/// before its contents are streamed, hashed, and rewritten into the temp file.
///
/// Returns the new `Blob`, any detected run-time references, and locations of any self-references.
fn make_content_addressed(
    file: &Path,
    out_dir: &Path,
    pkgs_dir: &Path,
    spec: &Spec,
) -> anyhow::Result<(Blob, BTreeSet<ObjectId>, BTreeSet<u64>)> {
    debug_assert!(file.starts_with(out_dir));
    debug_assert!(pkgs_dir.is_absolute());

    let mut reader = std::fs::File::open(file)?;
    let metadata = reader.metadata()?;
    let is_executable = metadata.mode() & 0o100 != 0;

    if is_executable {
        // If this is an ELF/Mach-O binary, patch out self-references to use relative paths. This
        // is much more convenient than using the zeroed-out install dir trick, and we can
        // thankfully afford to use it here.
        if let Some(kind) = infer::get_from_path(file)? {
            match kind.mime_type() {
                "application/x-executable" => patch_elf_rpaths_with_prefix(out_dir, file)?,
                "application/x-mach-binary" => patch_mach_rpaths_with_prefix(out_dir, file)?,
                _ => {}
            }
        }
    }

    let zeroed_install_dir = pkgs_dir.join(format!(
        "{}-{}-{}",
        spec.name,
        spec.version,
        ObjectId::zero()
    ));

    let temp_file = tempfile::NamedTempFile::new_in("/var/tmp")?;
    let hasher = HashWriter::with_hasher(Hasher::new_blob(is_executable), temp_file);
    let rewriter = RewriteSink::new(hasher, out_dir, &zeroed_install_dir)?;
    let mut writer = ReferenceSink::new(rewriter);

    // Rewrite any self-references to the install dir with a zeroed-out placeholder install dir.
    crate::copy_wide(&mut reader, &mut writer)?;
    writer.flush()?;

    let (rewriter, mut references) = writer.into_inner();
    let (hasher, offsets) = rewriter.into_inner()?;
    let object_id = hasher.object_id();
    let temp_file = hasher.into_inner();

    // Do not count the placeholder hash as a reference.
    references.remove(&ObjectId::zero());

    let metadata = temp_file.as_file().metadata()?;
    let blob = Blob::from_file_unchecked(temp_file, is_executable, metadata.len(), object_id);

    Ok((blob, references, offsets))
}

/// Patches all executable RPATHs that start with `prefix` to use relative paths.
///
/// This function only works on [ELF binaries](https://wiki.osdev.org/ELF).
fn patch_elf_rpaths_with_prefix(prefix: &Path, executable: &Path) -> anyhow::Result<()> {
    debug_assert!(prefix.is_absolute());
    debug_assert!(executable.is_absolute());
    debug_assert!(executable.starts_with(prefix));

    fn get_rpaths(exec: &Path) -> anyhow::Result<Vec<PathBuf>> {
        let mut cmd = Command::new(PATCHELF_BIN);
        let output = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .arg("--print-rpath")
            .arg(exec)
            .output()?;

        if output.status.success() {
            let stdout = std::str::from_utf8(&output.stdout)?;
            Ok(stdout.split(":").map(PathBuf::from).collect())
        } else {
            let stderr = std::str::from_utf8(&output.stderr)?;
            Err(anyhow!("{:?} returned non-zero status: [{}]", cmd, stderr))
        }
    }

    fn replace_prefix_with_origin(rpaths: &mut [PathBuf], prefix: &Path, origin: &Path) {
        for rpath in rpaths.iter_mut() {
            if rpath.starts_with(prefix) {
                let rel = pathdiff::diff_paths(&rpath, origin).expect("both paths are absolute");
                *rpath = Path::new("$ORIGIN").join(rel);
            }
        }
    }

    fn set_rpaths(exec: &Path, rpaths: Vec<PathBuf>) -> anyhow::Result<()> {
        let strings: Vec<_> = rpaths.iter().map(|p| p.display().to_string()).collect();
        let rpaths = strings.join(":");

        let mut cmd = Command::new(PATCHELF_BIN);
        let output = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .arg("--force-rpath") // TODO: Should we support more modern `RUNPATH`?
            .arg("--set-rpath")
            .arg(rpaths)
            .arg(exec)
            .output()?;

        if !output.status.success() {
            let stderr = std::str::from_utf8(&output.stderr)?;
            return Err(anyhow!("{:?} returned non-zero status: [{}]", cmd, stderr));
        }

        Ok(())
    }

    let mut rpaths = get_rpaths(executable)?;
    replace_prefix_with_origin(&mut rpaths, prefix, executable);
    set_rpaths(executable, rpaths)?;
    Ok(())
}

fn patch_mach_rpaths_with_prefix(prefix: &Path, executable: &Path) -> anyhow::Result<()> {
    debug_assert!(prefix.is_absolute());
    debug_assert!(executable.is_absolute());
    debug_assert!(executable.starts_with(prefix));

    unimplemented!()
}
