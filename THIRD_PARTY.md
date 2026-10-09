# Third-party licensing

The project's MIT license does not replace the licenses of its dependencies.
Dependencies are fetched by Cargo; their source is not vendored in this repository.

The dependency metadata in the current `Cargo.lock` includes:

- MIT and Apache-2.0, often as alternative license choices.
- MIT for `lz4` 1.28.1 and its `lz4-sys` 1.11.1+lz4-1.10.0 bindings.
  `lz4-sys` bundles the LZ4 1.10.0 library, whose library code uses BSD-2-Clause.
  The bundled library’s license and copyright notice must accompany binary
  redistribution; see `liblz4/lib/LICENSE` in the dependency source.
- Unicode-3.0 in addition to an MIT or Apache-2.0 choice for `unicode-ident`.
- Other alternatives or exceptions listed by individual packages, such as
  the Unlicense option for `memchr` and LLVM exceptions for some system bindings.

Inspect the exact dependency versions, declared licenses, and source locations
with `cargo metadata --locked --format-version 1`. The upstream license files
and copyright notices are authoritative; package metadata alone is not a
complete set of redistribution notices. When distributing binaries or vendored
source, include the notices and license texts required by the dependencies
actually distributed, including native code bundled by a dependency. Review
these again when changing versions, features, or target platforms.

The fixture documentation in [tests/fixtures/offline](tests/fixtures/offline/README.md)
records the kernel and Perfetto format references used for interoperability.
External validation tools such as Perfetto and libtraceevent are not distributed
with this repository and retain their own licenses.
