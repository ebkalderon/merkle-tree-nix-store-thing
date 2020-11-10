# Further design improvements

## Support for package builders

In order for this Merkle tree design to expand further, there needs to be a way
to represent build instructions and not just content-addressable packages alone.
The model we currently have is as follows:

Object  | Description
--------|-----------------------------------------------------------------------
Blob    | A single file or executable.
Tree    | A flat mapping of filenames to one or more blobs/trees.
Package | Equivalent to a Nix NAR (name, platform, references, root tree).

Ideally, we should adopt a model similar to the [Nix intensional store RFC] to
simultaneously support content-addressable outputs with Nix-style build recipes.

[Nix intensional store RFC]: https://github.com/NixOS/rfcs/pull/17

In order to support build instructions under our model, we will need to add two
more object types:

Object    | Description
----------|-----------------------------------------------------------------------
Builder   | A file with the name, description, platform, deps, build instructions.
Mapping   | A file mapping a package to a builder and contains some metadata.

Both of these objects live in the `objects` directory as well, with the file
extensions `.bld` and `.map` respectively.

### Examples

#### Example builder file (.bld)

```json
{
  "name": "foo-1.4.0",
  "platform": "x86_64-apple-darwin",
  "dependencies": ["badcfe2143658709"],
  "build-dependencies": ["0987654321fedcba"],
  "sources": {
    "foo.tar.gz": "abcdef1234567890"
  },
  "env": {
    "KEY": "VALUE"
  },
  "command": ["sh", "-c", "tar zxvf foo.tar.gz\nmake\n "]
}
```

#### Example mapping file (.map)

```json
{
  "builder": "abcdef1234567890",
  "result": "0987654321fedcba",
  "metadata": {
    "duration": "2h 37m",
    "timestamp": "2020-11-09T08:51:26+00:00"
  }
}
```

### Structure

An example of a full closure for a package could look like:

```text
<hash>.map
├── <hash>.pkg
│   └── <hash>.tree
│       ├── <hash>.tree
│       │   └── <hash>.blob
│       ├── <hash>.blob
│       └── <hash>.blob
└── <hash>.bld
    ├── <hash>.bld
    │   └── <hash>.bld
    ├── <hash>.bld
    │   ├── <hash>.bld
    │   └── <hash>.bld
    └── <hash>.bld
```

Builders can only ever reference other builders and not packages directly. To
bridge the gap, there exists a link between builders and packages in the form of
mappings, which are represented on disk as symlinks, e.g:

```text
# Single user mode, would be in ~/.config/foo/mappings in multi-user mode
<store>/mappings/
├── localhost
│   ├── ab
│   │   └── cdef1234567890
│   │       └── 0987654321fedcba # Symlink to "<store>/objects/badcfe2143658709.map"
│   └── 09
│       └── 87654321fedcba
│           └── abcdef1234567890 # Symlink to "<store>/objects/badcfe2143658709.map"
└── cache.foo.io
    ├── ab
    │   └── cdef1234567890
    │       └── 0987654321fedcba # Symlink to "<store>/objects/badcfe2143658709.map"
    └── 09
        └── 87654321fedcba
            └── abcdef1234567890 # Symlink to "<store>/objects/badcfe2143658709.map"
```

For every stored mapping that is added to the store, two symlinks are created in
the `mappings` directory: one with the hash of the builder and the other with
the hash of the resulting package. Both of these links point to the same `.map`
file, which contains both hashes plus some optional build metadata. This way, if
one of the hashes is known, the other can be queried quickly.

Mappings are a many-to-many relationship and duplicates can also exist across
each trusted source. Priority for which source's mappings should be used at
build time would be determined by the store's configuration. By default,
mappings from `localhost` would be prioritized over those from remote sources.

These mappings can be acquired in one of two ways:

1. Queried and copied from external trusted sources (cryptographically signed).
2. Executing the builder script in a temporary directory, writing the output
   to `<store>/packages/<name>-0000000000000000`, and recursing into the
   output directory tree. For every symlink found to point to a path inside the
   store, normalize it into a path relative to `<store>/packages` (not allowed
   to traverse outside this directory). For blob files that contain
   self-references to the build directory, patch them into relative paths using
   `patchelf` on Linux or `install_name_tool` on macOS in a similar manner to
   Spack ([details], [source]), before hashing. Once the output hash is
   determined, save the builder <-> content-addressable package mapping to disk.

[details]: https://lobste.rs/s/2lnncd/strategies_for_binary_relocation#c_btkgc0
[source]: https://github.com/spack/spack/blob/a80d221bfa1c9be4b2b9eff9f057edf62c34e50b/lib/spack/spack/relocate.py
