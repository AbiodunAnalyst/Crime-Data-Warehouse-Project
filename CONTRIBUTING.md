# Contributing

Contributions that improve reproducibility, data quality, documentation and analytical correctness are welcome.

## Issues

- Search existing issues first.
- Describe the affected pipeline stage.
- Include reproducible steps and non-sensitive example data where necessary.
- Never upload personal data, credentials or restricted source files.

## Pull requests

1. Keep the change focused.
2. Explain the affected data grain and assumptions.
3. Add validation for transformations or schema changes.
4. Update data provenance and documentation.
5. Confirm that no secret or restricted dataset is committed.

Suggested commit style:

```text
docs: document crime-data provenance and licence
refactor: move database configuration to environment variables
test: validate date-dimension key uniqueness
fix: prevent duplicate fact rows during incremental load
```

By contributing, you agree that your contribution may be distributed under the repository’s MIT licence.
