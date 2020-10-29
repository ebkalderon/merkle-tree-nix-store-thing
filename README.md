# Content addressable Nix-like store backed by a Merkle tree (PoC)

This is not a real build system nor package manager, only a crappy proof of
concept. It does not integrate hash rewriting (yet), making it an extensional
store design and not an intensional design.

## Features

* Every unique file in the store is stored as a sparse object in the
  content-addressable `objects` directory, similar to Git and OSTree. Hashes are
  based on BLAKE3 instead of SHA256 for performance and security.
* Realized derivations (aka "checkouts") are, ahem, _checked out_ into the
  `packages` directory.
* Files are transparently deduplicated on disk using hard links without
  requiring regular optimization passes, like Nix.

## Future work

* Copying closures over the network, although not implemented yet, could be made
  much more efficient under this model than standard Nix thanks to the magic of
  Merkle trees.
* Like Git and OSTree, the Merkle tree structure in the `objects` directory
  should enable efficient garbage collection to be implemented without requiring
  a shared `nix-daemon` nor a SQLite database to improve lookup performance.
* Hash rewriting could probably be made to work by adding the following
  additional object types:
  1. A type identical to a regular `Blob`, but with zeroed-out path hashes that
     can be substituted. Unlike regular `Blob` objects, these cannot be
     hard-linked nor checked out directly. They will need to be copied over to
     the final destination(s) and the zeroed-out placeholders will be replaced
     with the real hashes at instantiation time. Let's call this type
     `BlobModuloSelfRefs`, for the sake of example.
  2. A "ref" similar to a Git ref; maps onto a `BlobModuloSelfRefs` object hash.
     This hash corresponds to a `eqClass` or "equivalence class" hash as
     described in the "Intensional store" section of Eelco Dolstra's original
     Nix PhD thesis.
  3. An ordered list of hashes to substitute into a given `BlobModuloSelfRefs`
     file. The number of hashes in this object _must_ correspond to the number
     of zeroed out blanks in the `BlobModulo` object it's paired with. Let's
     call this object `SelfRefs`.

## Usage

```bash
$ mkdir -p store
$ cargo run
<snip>
$ tree ./store
store
├── objects
│   ├── 48
│   │   └── 642aa3b535c3d2b8223b4ae8b0f9a62f1d5f1c769d136cb0da301264649603.tree
│   ├── 9f
│   │   └── cb43efcfdcc0cffb5bb2c2b386c0e75c1d9e01321b0f62f2bf907ec6557f60.blob
│   ├── ab
│   │   └── 4db9898d125eb5f4b396fefd45b32fdb5a663e16855bcff29b9e18dc0f6f2f.blob
│   ├── ac
│   │   └── 0d0d568df5b048a83b2e1a6f81120266695d0f2d5843d3ed69bf4e5379146d.pkg
│   ├── c1
│   │   └── 7cb4d06cb51d69238b70e45766e9b265c7d70cb5c23e510ce2a940610c3e64.pkg
│   ├── ca
│   │   └── 290b71a756a43da3eaedcab8afbbe594af6f25dd696ce877c5ceb3fe24c892.blob
│   └── dc
│       └── 0675565eada2f4d0df31f5a7d8c0c06c256decd6404ba3eee560686374332f.tree
└── packages
    ├── bar-c17cb4d06cb51d69238b70e45766e9b265c7d70cb5c23e510ce2a940610c3e64
    │   └── main.rs
    └── foo-ac0d0d568df5b048a83b2e1a6f81120266695d0f2d5843d3ed69bf4e5379146d
        ├── bar.sh
        ├── baz.rs -> ./src/main.rs
        ├── foo.txt
        └── src
            └── main.rs

12 directories, 12 files
```
