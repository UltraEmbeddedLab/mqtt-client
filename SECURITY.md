# Security Policy

## Supported Versions

Security fixes land on the **latest minor release of the current major**. Older minors
receive fixes only for critical vulnerabilities, and only for six months after the next
minor ships. This is a rolling policy — it does not go stale as releases are cut.

| Version        | Supported                          |
|----------------|------------------------------------|
| Latest minor   | :white_check_mark: all fixes       |
| Previous minor | :warning: critical only, 6 months  |
| Older          | :x:                                |

Run `composer show ultraembeddedlab/mqtt-client` to see which you are on, and check
[releases](https://github.com/UltraEmbeddedLab/mqtt-client/releases) for the current minor.

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly.

**Do NOT open a public GitHub issue for security vulnerabilities.**

### How to Report

Preferred: [open a private security advisory](https://github.com/UltraEmbeddedLab/mqtt-client/security/advisories/new)
through GitHub Private Vulnerability Reporting. It keeps the report, the discussion and
the fix in one place and lets us issue a CVE.

Alternatively:

1. Email: **gewaldb@gmail.com**
2. Subject line: `[SECURITY] mqtt-client vulnerability report`
3. Include:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if any)

### Response Timeline

- **Acknowledgment**: Within 48 hours
- **Initial assessment**: Within 5 business days
- **Fix release**: Within 30 days for critical issues

### What to Expect

- We will acknowledge receipt of your report
- We will investigate and validate the vulnerability
- We will work on a fix and coordinate disclosure
- We will credit you in the release notes (unless you prefer anonymity)

### Scope

The following are in scope:

- MQTT protocol implementation vulnerabilities
- TLS/SSL configuration weaknesses
- Authentication bypass or credential exposure
- Buffer overflow or memory safety issues
- Injection attacks via topic names or payloads
- Denial of service via malformed packets

### Security Best Practices for Users

- Always use TLS in production (`useTls: true`)
- Enable peer verification (default: `verify_peer: true`, `verify_peer_name: true`)
- Use strong, unique client IDs
- Rotate credentials regularly
- Set appropriate `keepAlive` timeouts
- Use QoS 1 or 2 for critical messages
- Store session files in secure, non-public directories
- Never commit `.env` files with production credentials
