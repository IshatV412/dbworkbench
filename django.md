```markdown
# WAVE-DB Django Backend — Full Implementation Context and Instructions

---

## Project Background

WAVE-DB is a middleware/proxy layer that sits on top of any PostgreSQL 
database to provide versioning, rollback, commit tracking, and snapshot 
management — similar to Git but for databases.

The backend is a hybrid Django + FastAPI architecture:
- Django handles authentication, internal data models, ORM, admin panel
- FastAPI handles all HTTP endpoints, SQL execution, Kafka, rollback pipeline

You are implementing ONLY the Django portion. You will not touch the 
FastAPI codebase at all. Django runs as a standalone backend that FastAPI 
will call into.

---

## Critical Architecture Details

WAVE-DB connects to TWO databases simultaneously:
1. WAVE-DB's own internal PostgreSQL database — Django ORM manages this
2. The user's own external PostgreSQL database — FastAPI manages this

Django ORM only ever touches WAVE-DB's internal database.
The user's external database is never touched by Django.

---

## Project Structure to Create

django_backend/
├── authentication/
├── connections/
├── core/
├── manage.py
└── django_backend/
    └── settings.py

---

## Task 1 — Project Setup

Create a Django project called django_backend.
Create three apps: authentication, connections, core.

### settings.py configuration

INSTALLED_APPS must include:
- 'authentication'
- 'connections'
- 'core'
- 'rest_framework'
- 'rest_framework_simplejwt'
- 'django.contrib.admin'
- 'django.contrib.auth'
- all default Django apps

AUTH_USER_MODEL = 'authentication.User'
This must be set BEFORE any migrations are run. This is non negotiable.
Django's entire auth system, admin, sessions, and permissions reference
the User table. If you change the User model after the first migration,
foreign keys across all Django internal tables break and cannot be fixed
without wiping the database.

DATABASE configuration must point to WAVE-DB's own internal PostgreSQL
instance — not the user's database. Use environment variables for all
credentials.

All sensitive values must come from environment variables:
- SECRET_KEY
- DATABASE credentials — NAME, USER, PASSWORD, HOST, PORT
- FERNET_KEY — used for encrypting database connection passwords

Use python-dotenv or os.environ to load these. Never hardcode them.

Do NOT run any migrations until the custom User model is defined.

---

## Task 2 — Custom User Model

App: authentication
File: authentication/models.py

### Why this must be done first
Django's built in User table is referenced by foreign keys across many
internal Django tables — permissions, admin logs, sessions. Once the
first migration runs, changing the User model breaks all of these
relationships. The custom User model must exist before the first
migration.

### Implementation
Create a User model extending AbstractUser.
AbstractUser is the correct choice — not AbstractBaseUser.
AbstractUser keeps all of Django's default User fields and methods
(username, password, email, first_name, last_name, is_active, is_staff,
permissions machinery, password hashing) and simply allows extension.
AbstractBaseUser strips everything and requires building from scratch —
this is only needed when the login field or core structure needs to be
completely different, which is not the case here.

No extra fields are needed on the User model itself.
Database connection profiles are stored in a separate model linked via
ForeignKey — one user can have many connection profiles.

Register the User model with Django admin.

---

## Task 3 — Database Connection Profile Model

App: connections
File: connections/models.py

### Purpose
When a user wants to use WAVE-DB on their own PostgreSQL database, they
provide their database credentials. WAVE-DB stores these so FastAPI can
retrieve them to establish dynamic connections to the user's database.

### Fields
- name — CharField — human readable label for this connection
- host — CharField — PostgreSQL server address
- port — IntegerField — default 5432
- database_name — CharField — the specific database on that server
- db_username — CharField — PostgreSQL username
- db_password — CharField — PostgreSQL password, stored encrypted
- user — ForeignKey to AUTH_USER_MODEL with on_delete=CASCADE
- created_at — DateTimeField with auto_now_add=True

### Encryption — Critical
db_password must NEVER be stored in plain text.
Use Python's cryptography library — specifically Fernet symmetric
encryption.

Fernet works as follows:
- A secret key is generated once and stored in environment variables
- Encryption: Fernet(key).encrypt(password.encode()) before saving
- Decryption: Fernet(key).decrypt(encrypted_password).decode() when needed

Override the model's save() method to automatically encrypt db_password
before it is written to the database.

Create a get_decrypted_password() method on the model that FastAPI calls
when it needs the actual password to establish a database connection.

The Fernet key must be loaded from environment variables — never
hardcoded in the codebase.

### Relationship
One user can have many ConnectionProfiles.
ForeignKey sits on ConnectionProfile pointing to User.
This means if a user is deleted, all their connection profiles are
deleted with them via CASCADE.

### Admin Configuration
- list_display: name, host, database_name, user, created_at
- list_filter: user
- search_fields: host, database_name
- readonly_fields: db_password — the encrypted password must never be
  editable directly through admin

---

## Task 4 — Core ORM Models

App: core
File: core/models.py

These four models form the entire internal versioning system of WAVE-DB.
Every SQL execution, inverse operation, snapshot, and configuration
record is stored here.

### CommitEvent
Records every modifying SQL command that was executed against a user's
database.

Fields:
- version_id — CharField or UUIDField — db_index=True — unique
  identifier for this commit — indexed because almost every FastAPI
  query filters or orders by version_id
- timestamp — DateTimeField — auto_now_add=True
- sql_command — TextField — the SQL that was executed
- status — CharField — success or failed
- user — ForeignKey to AUTH_USER_MODEL with on_delete=CASCADE
- connection_profile — ForeignKey to ConnectionProfile
  with on_delete=CASCADE

Add a composite index on (user, timestamp) in the Meta class using
Meta.indexes with models.Index. This speeds up the commit history view
in the Web UI which queries by user and orders by timestamp.

### InverseOperation
Stores the reverse SQL for every commit. Used during rollback to undo
changes.

Fields:
- version_id — CharField — db_index=True — must match the version_id
  of its CommitEvent
- inverse_sql — TextField — the SQL that undoes the original command
- commit — OneToOneField to CommitEvent with on_delete=CASCADE —
  each commit has exactly one inverse operation

### Snapshot
Records a full database snapshot taken periodically. Actual snapshot
data lives in AWS S3 — this model only stores the reference to it.

Fields:
- snapshot_id — UUIDField — auto generated — unique
- version_id — CharField — db_index=True — the commit version at which
  this snapshot was taken — indexed because rollback queries constantly
  filter snapshots by version range
- created_at — DateTimeField — auto_now_add=True
- s3_key — CharField — the S3 object key/path where snapshot data is
  stored — NOT the snapshot data itself
- connection_profile — ForeignKey to ConnectionProfile
  with on_delete=CASCADE

### SnapshotPolicy
Controls how frequently snapshots are taken for each connection profile.

Fields:
- frequency — IntegerField — number of commits between each snapshot
- last_updated — DateTimeField — auto_now=True
- connection_profile — OneToOneField to ConnectionProfile
  with on_delete=CASCADE — one policy per connection profile

### Why db_index=True on version_id matters
The rollback pipeline performs these queries constantly:
- Find all CommitEvents between version X and version Y
- Find the nearest Snapshot before version X
- Find all InverseOperations between version X and version Y
All three are range filters on version_id. Without an index these
queries get slower as the table grows. With an index they stay fast
regardless of table size.

### Admin Configuration

CommitEvent:
- list_display: version_id, user, timestamp, status, connection_profile
- list_filter: user, status, timestamp
- search_fields: sql_command, version_id

InverseOperation:
- list_display: version_id, commit_event
- search_fields: version_id

Snapshot:
- list_display: snapshot_id, version_id, created_at, connection_profile
- list_filter: connection_profile
- search_fields: version_id

SnapshotPolicy:
- list_display: connection_profile, frequency, last_updated

---

## Task 5 — Migrations and Indexing

Run in this exact order:
1. python manage.py makemigrations authentication
2. python manage.py makemigrations connections
3. python manage.py makemigrations core
4. python manage.py migrate

After migration verify:
- version_id has db_index=True on CommitEvent, InverseOperation, Snapshot
- Composite index on CommitEvent (user, timestamp) exists in Meta.indexes

---

## Task 6 — Atomic Write Service Function

App: core
File: core/services.py

### Purpose
After FastAPI executes a SQL command against the user's database, it
needs to persist three things into WAVE-DB's internal database:
1. CommitEvent — record of what was executed
2. InverseOperation — how to undo it
3. Snapshot — full backup if policy frequency condition is met

These three writes must happen atomically — all succeed together or none
are committed. If InverseOperation write fails after CommitEvent succeeds,
you have a commit with no undo operation, which makes rollback impossible
for that version. This is a corrupt state.

### Implementation
Write a single function that accepts:
- version_id
- sql_command
- inverse_sql
- user
- connection_profile
- status

Inside the function use Django's transaction.atomic() as a context manager.
Within the atomic block:
1. Create CommitEvent record
2. Create InverseOperation record linked to CommitEvent
3. Read SnapshotPolicy frequency for this connection_profile
4. Count commits since last snapshot
5. If count meets frequency threshold, create Snapshot record with s3_key
   placeholder — actual S3 upload is handled by FastAPI, Django only
   stores the metadata record

If any write fails, transaction.atomic() automatically rolls back all
writes in the block. Raise the exception so FastAPI can catch it and
return an appropriate error to the user.

---

## Task 7 — SimpleJWT Setup

App: authentication
File: authentication/urls.py and django_backend/settings.py

### Purpose
Django issues JWT tokens when users log in. FastAPI validates these
tokens on every incoming request independently — without calling back
to Django. This means auth is handled at the FastAPI layer with zero
database overhead.

### Settings Configuration
Add to settings.py:

SIMPLE_JWT = {
    'ACCESS_TOKEN_LIFETIME': timedelta(minutes=15),
    'REFRESH_TOKEN_LIFETIME': timedelta(days=7),
    'ALGORITHM': 'HS256',
    'SIGNING_KEY': env('JWT_SECRET_KEY'),
    'AUTH_HEADER_TYPES': ('Bearer',),
}

JWT_SECRET_KEY must match the secret key FastAPI uses to validate tokens.
Store it in environment variables.

### Endpoints
Expose two endpoints in authentication/urls.py:
- POST /auth/token — accepts username and password, returns access
  and refresh token pair
- POST /auth/token/refresh — accepts refresh token, returns new
  access token

### Custom Token Payload
Customize the token to include user_id and username in the payload.
FastAPI reads these from the decoded token to identify who is making
the request without a database call.

Create a custom token serializer extending TokenObtainPairSerializer
and override get_token() to add username to the token claims.

---

## Task 8 — Admin Panel Verification

- Run python manage.py createsuperuser
- Log into /admin
- Verify all models are visible:
  - User
  - ConnectionProfile
  - CommitEvent
  - InverseOperation
  - Snapshot
  - SnapshotPolicy
- Verify list_display columns appear correctly for each model
- Verify list_filter sidebar works on models where configured
- Verify search_fields search box works on each model
- Verify db_password is readonly and not editable in ConnectionProfile

---

## Implementation Order — Follow Exactly

1. Create Django project and three apps
2. Configure settings.py — especially AUTH_USER_MODEL before migrations
3. Create custom User model in authentication app
4. Run first migration — authentication only
5. Create ConnectionProfile model with Fernet encryption in connections app
6. Create all four core models in core app
7. Run makemigrations for connections and core
8. Run migrate
9. Verify all indexes
10. Write atomic service function in core/services.py
11. Configure SimpleJWT in settings and authentication urls
12. Create superuser and verify admin panel

---

## Dependencies to Install

django
djangorestframework
djangorestframework-simplejwt
cryptography
psycopg2-binary
python-dotenv

---

## Important Rules

- Never hardcode secrets, keys, or credentials anywhere in the code
- Never store db_password in plain text — always encrypt with Fernet
- Never run migrate before AUTH_USER_MODEL is set in settings
- Never let Django ORM touch the user's external database —
  Django only ever reads and writes to WAVE-DB's own internal database
- The atomic service function must always write CommitEvent and
  InverseOperation together — never one without the other
```