//! Types for supported hardware platforms.

use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use anyhow::anyhow;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

#[cfg(target_arch = "x86")]
const HOST_ARCH: Arch = Arch::I686;
#[cfg(target_arch = "x86_64")]
const HOST_ARCH: Arch = Arch::X86_64;
#[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
compile_error!("unsupported system architecture");

#[cfg(all(target_os = "macos"))]
const HOST_OS: Os = Os::Darwin;
#[cfg(all(target_os = "linux", target_env = "gnu"))]
const HOST_OS: Os = Os::Linux(Env::Gnu);
#[cfg(all(target_os = "linux", target_env = "musl"))]
const HOST_OS: Os = Os::Linux(Env::Musl);
#[cfg(all(target_os = "linux", target_env = ""))]
compile_error!("unsupported Linux environment");
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
compile_error!("unsupported operating system");

/// Macro for declaring constant `Platform` objects from target triples.
///
/// # Example
///
/// ```rust
/// # use foo::{platform, Platform};
/// const EXAMPLE: Platform = platform!(x86_64-linux-gnu);
/// ```
#[macro_export]
macro_rules! platform {
    ($arch:ident - $($os:ident)-+) => {
        $crate::platform::Platform {
            arch: $crate::platform_inner!(@arch $arch),
            os: $crate::platform_inner!(@os $($os)-+),
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! platform_inner {
    (@arch i686) => {
        $crate::platform::Arch::I686
    };
    (@arch x86_64) => {
        $crate::platform::Arch::X86_64
    };
    (@os darwin) => {
        $crate::platform::Os::Darwin
    };
    (@os linux-gnu) => {
        $crate::platform::Os::Linux($crate::platform::Env::Gnu)
    };
    (@os linux-musl) => {
        $crate::platform::Os::Linux($crate::platform::Env::Musl)
    };
}

/// A supported hardware target "triple".
#[derive(Clone, Copy, Debug, Hash, PartialEq)]
pub struct Platform {
    /// The processor architecture.
    pub arch: Arch,
    /// The operating system, and optionally its `libc` environment.
    pub os: Os,
}

impl Platform {
    /// Returns the platform of the current host.
    pub const fn host() -> Self {
        Platform {
            arch: HOST_ARCH,
            os: HOST_OS,
        }
    }
}

impl FromStr for Platform {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.splitn(2, "-");

        let arch = parts.next().ok_or(anyhow!("expected platform string"))?;
        let os = parts.next().ok_or(anyhow!("expected operating system"))?;

        Ok(Platform {
            arch: arch.parse()?,
            os: os.parse()?,
        })
    }
}

impl Display for Platform {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}-{}", self.arch, self.os)
    }
}

impl<'de> Deserialize<'de> for Platform {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(de::Error::custom)
    }
}

impl Serialize for Platform {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

/// A list of supported processor architectures.
#[derive(Clone, Copy, Debug, Hash, PartialEq)]
pub enum Arch {
    /// Intel i686 architecture (32-bit).
    I686,
    /// AMD X86_64 architecture (64-bit).
    X86_64,
}

impl FromStr for Arch {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "i686" => Ok(Arch::I686),
            "x86_64" => Ok(Arch::X86_64),
            arch => Err(anyhow!("unsupported CPU architecture {:?}", arch)),
        }
    }
}

impl Display for Arch {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Arch::I686 => f.write_str("i686"),
            Arch::X86_64 => f.write_str("x86_64"),
        }
    }
}

/// A list of supported operating systems.
#[derive(Clone, Copy, Debug, Hash, PartialEq)]
pub enum Os {
    /// Apple Darwin (macOS, iOS).
    Darwin,
    /// Linux with either `glibc` or `musl`.
    Linux(Env),
}

impl FromStr for Os {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.splitn(2, "-");

        match parts.next() {
            Some("darwin") => Ok(Os::Darwin),
            Some("linux") => parts
                .next()
                .ok_or(anyhow!("missing system environment"))
                .and_then(|s| s.parse())
                .map(Os::Linux),
            Some(os) => Err(anyhow!("unsupported operating system {:?}", os)),
            None => Err(anyhow!("expected non-empty string")),
        }
    }
}

impl Display for Os {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Os::Darwin => f.write_str("darwin"),
            Os::Linux(env) => write!(f, "linux-{}", env),
        }
    }
}

/// A list of supported C library environments.
#[derive(Clone, Copy, Debug, Hash, PartialEq)]
pub enum Env {
    /// `glibc` environment.
    Gnu,
    /// `musl` environment.
    Musl,
}

impl FromStr for Env {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "gnu" => Ok(Env::Gnu),
            "musl" => Ok(Env::Musl),
            env => Err(anyhow!("unsupported libc environment {:?}", env)),
        }
    }
}

impl Display for Env {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Env::Gnu => f.write_str("gnu"),
            Env::Musl => f.write_str("musl"),
        }
    }
}
