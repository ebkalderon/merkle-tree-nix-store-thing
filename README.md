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
│   ├── 62
│   │   └── c9a72b3b8455ad838992cb8003146349999d3355eaf100e603925a4c0340d8.tree
│   ├── 97
│   │   └── ba70e671c28d5e04bc85b549696520db72612a8919f0b79d3f47d6c746bf91.pkg
│   ├── 9f
│   │   └── cb43efcfdcc0cffb5bb2c2b386c0e75c1d9e01321b0f62f2bf907ec6557f60.blob
│   ├── ab
│   │   └── 4db9898d125eb5f4b396fefd45b32fdb5a663e16855bcff29b9e18dc0f6f2f.blob
│   ├── c7
│   │   └── 8e56cd02199f7ca9d2c8bb4fafe99ad769a29cb4ca5f56dea0eee64fb8a061.tree
│   ├── ca
│   │   └── 290b71a756a43da3eaedcab8afbbe594af6f25dd696ce877c5ceb3fe24c892.blob
│   └── e9
│       └── cf339074177568f30d96c56aa1e729b6bb21cbea5f3d835bdae717c139c65a.pkg
└── packages
    ├── bar-e9cf339074177568f30d96c56aa1e729b6bb21cbea5f3d835bdae717c139c65a
    │   └── main.rs
    └── foo-97ba70e671c28d5e04bc85b549696520db72612a8919f0b79d3f47d6c746bf91
        ├── bar.sh
        ├── baz.rs -> ./src/main.rs
        ├── foo.txt
        └── src
            └── main.rs

12 directories, 12 files
```
