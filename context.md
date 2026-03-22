# CLAUDE.md — WEAVE-DB

## What This Project Is

WEAVE-DB (Workbench for Event-based Atomic Versioning of Databases) is a middleware layer that sits on top of any PostgreSQL database and provides Git-like version control — commit tracking, rollback via inverse operations, and periodic snapshots. Users bring their own PostgreSQL credentials, connect through a web-based SQL workbench, and every modifying SQL command they execute gets versioned. They can roll back to any previous version.

This is an academic software engineering project built by a four-person team: Anirudh, Ishat, Ishita, and Parrva (Shrey). The repo is `DBWORKBENCH`.

---

## The Core Concept — Dual Database

The single most important architectural fact: WEAVE-DB operates on **two databases simultaneously**.

The **user's external PostgreSQL** is where actual SQL commands run — INSERT, UPDATE, DELETE, SELECT, ALTER, DROP. WEAVE-DB never owns this database. The user provides credentials and WEAVE-DB connects dynamically.

**weavedb_internal** is WEAVE-DB's own PostgreSQL database. It stores everything the system needs to function — users, connection profiles, commit history, inverse operations, snapshots, snapshot policies. Django ORM manages this exclusively.

These two databases must never be confused. Django ORM never touches the user's database. The execution layer (FastAPI) never writes directly to weavedb_internal — it calls into Django's service layer for that.

---

## How Version Control Works

When a user submits a modifying SQL command through the workbench:

1. The command is validated and executed against the user's database
2. A **CommitEvent** is created with a unique version_id, timestamp, the SQL text, and status
3. An **InverseOperation** is generated — the reverse SQL that undoes the command (e.g., DELETE undoes INSERT)
4. CommitEvent and InverseOperation are written **atomically** — both succeed or neither is saved. A commit without an inverse is a corrupt state because that version becomes unrollbackable
5. The system checks the **SnapshotPolicy** for this connection. If enough commits have accumulated since the last snapshot, a **Snapshot** record is created (actual snapshot data goes to S3, the model just stores the reference)

When a user requests rollback to version X:

1. Find the nearest Snapshot before version X
2. Restore that snapshot
3. Replay all InverseOperations between the snapshot and the target version in correct order
4. If any inverse operation fails, rollback impact is limited to the interval between two adjacent snapshots

SELECT queries are executed but never tracked or versioned.

---

## Architecture — Django + FastAPI Hybrid

The backend is split across two frameworks running as separate processes.

**Django** handles authentication, JWT token issuance, all internal ORM models, the atomic write service function, Fernet encryption of stored credentials, and the admin panel.

**FastAPI** handles all HTTP endpoints exposed to the Web UI, SQL execution against user databases, dynamic connection management, Kafka integration for event ordering, inverse operation generation, the rollback pipeline, and real-time progress streaming.

This split exists because three requirements couldn't be met by a single framework: immediate SQL response (rules out task queues), real-time rollback streaming (rules out synchronous Django), and transactionally safe internal writes (rules out skipping Django ORM). The team evaluated pure Django, Celery, Django Channels, pure FastAPI + SQLAlchemy, and other alternatives before settling on this.

**Integration contract between the two:**
- Shared `JWT_SECRET_KEY` and `HS256` algorithm — Django issues tokens, FastAPI validates them independently
- JWT payload contains `user_id` and `username` — FastAPI reads these to identify the caller without a database lookup
- FastAPI calls Django's `record_commit()` function in `core/services.py` to persist versioning data atomically

---

## Key Models and Their Purpose

**User** (authentication app) — extends Django's AbstractUser with no extra fields. The custom model exists because AUTH_USER_MODEL must be set before the first migration and can never be changed after.

**ConnectionProfile** (connections app) — stores a user's external PostgreSQL credentials. The password field is encrypted with Fernet symmetric encryption on save and decrypted only when FastAPI needs to establish a connection. One user can have many connection profiles. If the user is deleted, all their profiles cascade-delete.

**CommitEvent** (core app) — records every modifying SQL command executed against a user's database. version_id is indexed because the rollback pipeline constantly does range queries on it. There's a composite index on (user, timestamp) for the commit history UI view.

**InverseOperation** (core app) — stores the reverse SQL for a commit. OneToOne relationship with CommitEvent — every commit has exactly one inverse, no exceptions.

**Snapshot** (core app) — records a full database snapshot reference. Stores the S3 key, not the actual data. version_id is indexed for rollback range queries.

**SnapshotPolicy** (core app) — controls snapshot frequency per connection profile. OneToOne with ConnectionProfile. The frequency field defines how many commits between snapshots.

---

## The Atomic Write — Why It Matters

`core/services.py` contains `record_commit()`, which writes CommitEvent, InverseOperation, and conditionally Snapshot inside `transaction.atomic()`. If any write fails, all are rolled back. This prevents the most dangerous corruption scenario: a commit existing without its inverse operation.

---

## Security Model

- All secrets (Django SECRET_KEY, database credentials, FERNET_KEY, JWT_SECRET_KEY) come from environment variables via `.env` — nothing is hardcoded
- User database passwords are encrypted at rest with Fernet. The key is generated once and stored in env vars. Double-encryption is prevented by checking if the value already starts with `gAAAAA` (Fernet encoding prefix)
- JWT access tokens expire in 15 minutes, refresh tokens in 7 days
- Authentication is required before any command execution (NFR-SEC-1)

---

## SRS Requirements Summary

The SRS defines these as the core functional requirements:

- REQ-1: Record every successful write as a commit entry
- REQ-2: Never track SELECT queries
- REQ-3: Each commit includes version_id, timestamp, and SQL command
- REQ-5: Maintain strictly ordered commit history with monotonically increasing version IDs
- REQ-7: Store one inverse operation per commit
- REQ-8: Mandatory snapshots at user-configurable intervals (default every 5 commits)
- REQ-9: During rollback, restore the nearest snapshot before target version
- REQ-10: Replay inverse operations between snapshot and target version
- REQ-11: If inverse operation fails, limit rollback damage to the interval between two adjacent snapshots
- REQ-14: Query execution engine that returns structured outputs
- REQ-16: Deterministic total order of concurrent write operations
- NFR-SEC-1: Authentication before command execution

Non-functional: latency is not a primary concern. Storage growth should be linear with commits. System reliability should be 99%+ for valid queries.

---

## Sprint Plan and Current Status (March 2026)

**Sprint 1 — Database Setup & Core Data Ops** (completed Feb 2026): PostgreSQL setup, backend SQL execution, commit tracking data model, snapshot storage stubs.

**Sprint 2 — Concurrency & Versioning/Rollback Core** (ongoing since late Feb 2026): Kafka event ordering, inverse operation generation, rollback engine (snapshot restore + inverse replay), snapshot frequency configuration. Django backend models and service layer are implemented. Anti-command generation and rollback engine are in progress on the FastAPI side.

**Sprint 3 — Frontend Integration & UI Workbench** (not started): Web UI with SQL editor, output panel, commit timeline, rollback workflow.

---

## Tech Stack

- **Django** — ORM, auth, admin, internal data management
- **FastAPI** — async HTTP endpoints, SQL execution, Kafka, rollback pipeline, streaming
- **PostgreSQL** — both internal (weavedb_internal) and user's external databases
- **SimpleJWT** — token-based authentication
- **Fernet (cryptography library)** — symmetric encryption for stored credentials
- **Apache Kafka** — event streaming for serializing concurrent writes
- **AWS S3** — snapshot storage
- **Trello** — project management (Kanban boards per sprint)

---

## Hard Rules — Never Violate

1. Never hardcode secrets, keys, or credentials
2. Never store db_password in plain text — always Fernet-encrypt
3. Never run migrations before AUTH_USER_MODEL is set
4. Never let Django ORM touch the user's external database
5. Never write CommitEvent without InverseOperation — always use the atomic service function
6. JWT_SECRET_KEY must be identical in Django and FastAPI
7. Snapshot model stores an S3 key reference, never actual snapshot data

---

## Naming Conventions (from SRS)

- Classes: PascalCase, singular nouns (e.g., `CommitEvent`, `RollbackManager`)
- Methods: camelCase, verb-noun pairs (e.g., `executeWrite()`, `loadSnapshot()`)
- Variables: camelCase, descriptive nouns (e.g., `versionId`, `sqlCommand`)
- Constants: UPPER_CASE (e.g., `DEFAULT_FREQ`, `MAX_LIMIT`)
- DB tables: lower_case, plural nouns (e.g., `commit_events`, `snapshots`)
- Use TODO and FIXME in comments. Include author name on non-trivial comments.

Note: Django's Python code follows Python conventions (snake_case for methods/variables) rather than the camelCase in the SRS, which was written with a language-agnostic style. The DB table naming applies.