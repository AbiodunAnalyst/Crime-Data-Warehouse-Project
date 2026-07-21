# Security and privacy policy

## Reporting

Use a public issue only for non-sensitive observations. Contact the repository owner privately for exposed credentials, private data or exploitable vulnerabilities.

## Required controls

- Keep PostgreSQL credentials in environment variables or an ignored local configuration file.
- Never commit `.env`, passwords, connection strings or private datasets.
- Use a least-privileged database account for development.
- Validate uploaded files and expected schemas.
- Aggregate outputs so the application does not expose individual-level information.
- Remove or rotate any credential that has previously been committed.
