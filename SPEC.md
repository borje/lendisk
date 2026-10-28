# lendisk — specification

The specification is two documents with different change rules:

- **[REQUIREMENTS.md](REQUIREMENTS.md)** — what the product does for its
  users, stated without naming any library, language, transport, storage
  engine or wire protocol. Principles P-1 to P-9, requirements with
  stable IDs (ID-, PEER-, EP-, REPL-, LEND-, RET-, VER-, NET-, OPS-,
  SEC-, COMPAT-, S3-), release gates G-1 to G-3 and the acceptance
  scenarios. Changing it changes the product; it gets a new version
  number and a changelog entry each time.
- **[DESIGN.md](DESIGN.md)** — how it is built: decisions D2 to D13 with
  the alternatives they beat, a per-requirement account of how each one
  is realised (components, ports, paths, headers, algorithms), the
  capacity derivation, and the open gates on the current design. Any of
  it may be replaced whenever a better way to meet the requirements
  appears, without touching REQUIREMENTS.md.

The test that decides where a sentence goes: would it have to change if
a different component, protocol or language were chosen? If yes, it is
design. Design references requirements by ID; requirements never
reference design.

Both are at draft v0.7, 2026-10-28, split from SPEC v0.6. `architecture.html`
is an overview rendered from the design.
