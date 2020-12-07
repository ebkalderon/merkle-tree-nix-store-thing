//! Newtypes for enforcing name requirements.

use std::fmt::{self, Display, Formatter};
use std::path::Path;
use std::str::FromStr;

use anyhow::anyhow;
use serde::{de, Deserialize, Deserializer, Serialize};
use smol_str::SmolStr;

use super::ObjectId;

/// The human-readable name of a package.
#[derive(Clone, Debug, Hash, Serialize)]
pub struct PackageName(SmolStr);

impl PackageName {
    /// The maximum acceptable name length (191 characters).
    ///
    /// This number was chosen in order to accommodate the limitations of common filesystems. In
    /// this case, the `ext4` filesystem enforces a file name length limit of 256 characters, and
    /// we also need to ensure we can fit a hyphen and the package's cryptographic hash (64).
    pub const MAX: usize = 256 - 1 - ObjectId::STR_LENGTH;

    /// Parses a package name from the string.
    ///
    /// # Errors
    ///
    /// This function will return an error if the string is empty, exceeds [`PackageName::MAX`] in
    /// length, contains invalid characters outside of `[A-Za-z0-9][+-._?=]`, or starts with a `.`
    /// character (for security reasons).
    pub fn parse<T: AsRef<str>>(s: T) -> anyhow::Result<Self> {
        if s.as_ref().is_empty() {
            return Err(anyhow!("package name cannot be empty"));
        }

        if s.as_ref().len() > Self::MAX {
            return Err(anyhow!(
                "package name must be shorter than {} characters",
                Self::MAX
            ));
        }

        if s.as_ref().starts_with('.') {
            return Err(anyhow!("package name cannot start with a `.` character"));
        }

        if !s.as_ref().chars().all(is_package_name) {
            return Err(anyhow!(
                "package name {:?} contains at least one invalid character",
                s.as_ref()
            ));
        }

        Ok(PackageName(SmolStr::new(s)))
    }
}

impl AsRef<str> for PackageName {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl<'de> Deserialize<'de> for PackageName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = std::borrow::Cow::<'de>::deserialize(deserializer)?;
        PackageName::parse(s).map_err(de::Error::custom)
    }
}

impl Display for PackageName {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for PackageName {
    type Err = anyhow::Error;

    /// Equivalent to [`PackageName::parse()`].
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        PackageName::parse(s)
    }
}

/// Returns `true` if `c` is considered a valid package name character.
#[inline]
pub(crate) fn is_package_name(c: char) -> bool {
    c.is_ascii_alphanumeric() || "+-._?=".contains(c)
}

/// Directory name of an installed package.
///
/// This is the human-readable name of the package concatenated with its object ID, separated
/// by a hyphen. Installed packages are located in the `packages` directory, and their file
/// contents may reference paths in other packages' directories via absolute paths.
///
/// `InstallName` implements `AsRef<Path>` so it can be treated identically to `std::path::Path`.
///
/// # Example
///
/// Given an example package named `hello-1.0.0`, its install name string could be:
///
/// ```text
/// hello-1.0.0-58f16e6023968d08d72b0554e8310f073365a0a62ab33bc9052d1f7a5510035b
/// ```
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct InstallName(String);

impl InstallName {
    /// Computes the directory name where the package should be installed.
    pub(super) fn new(name: &PackageName, id: ObjectId) -> Self {
        InstallName(format!("{}-{}", name, id))
    }

    /// Returns the human-readable name component of the `InstallName`.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use foo::{platform, Package};
    /// #
    /// # let pkg = Package {
    /// #     name: "hello-1.0.0".parse().unwrap(),
    /// #     system: platform!(x86_64-linux-gnu),
    /// #     references: Default::default(),
    /// #     self_references: Default::default(),
    /// #     tree: "0000000000000000000000000000000000000000000000000000000000000000".parse().unwrap(),
    /// # };
    /// #
    /// let install_name = pkg.install_name();
    /// assert_eq!(install_name.name(), "hello-1.0.0");
    /// ```
    pub fn name(&self) -> &str {
        self.0.rsplitn(2, '-').nth(1).unwrap()
    }

    /// Returns the package ID component of the `InstallName`.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use foo::{platform, ObjectId, Package};
    /// #
    /// # let pkg = Package {
    /// #     name: "hello-1.0.0".parse().unwrap(),
    /// #     system: platform!(x86_64-linux-gnu),
    /// #     references: Default::default(),
    /// #     self_references: Default::default(),
    /// #     tree: "0000000000000000000000000000000000000000000000000000000000000000".parse().unwrap(),
    /// # };
    /// #
    /// let install_name = pkg.install_name();
    /// let id: ObjectId = "58f16e6023968d08d72b0554e8310f073365a0a62ab33bc9052d1f7a5510035b".parse().unwrap();
    /// assert_eq!(install_name.id(), id);
    /// ```
    pub fn id(&self) -> ObjectId {
        self.0.rsplitn(2, '-').nth(0).unwrap().parse().unwrap()
    }
}

impl AsRef<Path> for InstallName {
    fn as_ref(&self) -> &Path {
        self.0.as_ref()
    }
}

impl Display for InstallName {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<InstallName> for String {
    fn from(name: InstallName) -> Self {
        name.0
    }
}
