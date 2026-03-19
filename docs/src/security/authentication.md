# Authentication

DTFS uses JWT (JSON Web Token) authentication for the REST API and WebUI. Passwords are hashed with bcrypt before storage.

## WebUI and REST API Authentication

The manager's REST API (default port 9402) serves both the WebUI dashboard and programmatic API endpoints. Access to state-mutating endpoints requires a valid JWT token in the `Authorization` header.

### Login Flow

```text
  Browser / CLI                          Manager REST API
       |                                       |
       |  POST /api/v1/auth/login              |
       |  {"username": "admin",                |
       |   "password": "..."}                  |
       |-------------------------------------->|
       |                                       |  Verify password against bcrypt hash
       |                                       |  Generate JWT with claims
       |  200 OK                               |
       |  {"token": "eyJ..."}                  |
       |<--------------------------------------|
       |                                       |
       |  GET /api/v1/cluster/status           |
       |  Authorization: Bearer eyJ...         |
       |-------------------------------------->|
       |                                       |  Validate JWT signature and expiry
       |  200 OK                               |
       |  {...}                                |
       |<--------------------------------------|
```

### JWT Token Structure

Tokens are signed using HMAC-SHA256 with a server-side secret. The payload contains:

| Claim | Description |
|-------|-------------|
| `sub` | Username |
| `role` | User role (`admin` or `readonly`) |
| `iat` | Issued-at timestamp (Unix epoch) |
| `exp` | Expiration timestamp (default: 24 hours after issuance) |

### Token Configuration

JWT behavior is configured in the manager TOML file:

```toml
[auth]
enabled = true
jwt_secret = "a-strong-random-secret-for-signing-tokens"
token_expiry_hours = 24
```

When `auth.enabled` is `false`, all API endpoints are accessible without authentication. This is intended for development only.

## Password Hashing

User passwords are hashed with bcrypt using the `bcrypt` crate. Bcrypt is a deliberately slow hashing algorithm that makes brute-force attacks impractical.

| Parameter | Value |
|-----------|-------|
| Algorithm | bcrypt |
| Cost factor | Default (currently 12 rounds) |
| Salt | Randomly generated per password |

Plaintext passwords are never stored or logged.

## Admin Credentials

### Default Admin Account

On first startup, the manager creates a default admin account if no users exist:

| Field | Default Value |
|-------|---------------|
| Username | `admin` |
| Password | `admin` |
| Role | `admin` |

The default password should be changed immediately after the first login.

### Changing the Admin Password

The admin password can be set in the manager configuration file:

```toml
[auth]
admin_username = "admin"
admin_password_hash = "$2b$12$..."  # bcrypt hash
```

To generate a bcrypt hash for a new password, use the `dtfs-cli` or any bcrypt tool:

```bash
# Using htpasswd (Apache utilities)
htpasswd -nbBC 12 "" "new-password" | cut -d: -f2

# Or set the plaintext password (hashed on first startup)
[auth]
admin_password = "new-secure-password"
```

When `admin_password` is set as plaintext, the manager hashes it with bcrypt on startup and uses the hash for all subsequent comparisons.

## Authorization

DTFS supports two roles:

| Role | Permissions |
|------|-------------|
| `admin` | Full access: read, write, configuration changes, maintenance operations |
| `readonly` | Read-only access: cluster status, node info, file listings, audit log queries |

State-mutating operations (maintenance mode, configuration updates, scrub triggers) require the `admin` role.

## API Authentication with dtfs-cli

The CLI tool authenticates against the manager REST API. The manager address and credentials can be provided via command-line flags or environment variables:

```bash
# Via environment variables
export DTFS_MANAGER="manager-01:9402"
export DTFS_TOKEN="eyJ..."

# Via command-line flags
dtfs-cli --manager manager-01:9402 status
```

For interactive use, the CLI can obtain a token by prompting for credentials, then caches the token for subsequent commands.

## Security Recommendations

- Change the default admin password immediately after installation.
- Use a strong, randomly generated `jwt_secret` (at least 32 characters).
- Set `token_expiry_hours` to the shortest practical duration for your environment.
- Enable TLS on the API server to protect tokens in transit (see [TLS](./tls.md)).
- In production, always set `auth.enabled = true`.
- Rotate the `jwt_secret` periodically; note that this invalidates all existing tokens.
