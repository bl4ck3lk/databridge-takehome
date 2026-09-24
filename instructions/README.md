# DataBridge: A Data Integration Service

## Overview

Build a **web service** called **DataBridge** for managing data connections and transferring files
between them. Instead of a CLI, you'll expose this functionality over HTTP and interact with it through
API requests. The requirements are outlined below and broken into three phases.
Phases 1 & 2 are required and expected to be completed; Phase 3 is optional.

**Expectation:** We value quality over completeness -- it is better to finish Phases 1 and 2 well than
to rush through all three phases. Use of AI tooling is acceptable and expected, see the [AI Collaboration](#ai-collaboration) section.

## Deliverables

Your submission must include:

1. **A working web service** that implements the requirements below.
2. **A `README`** in your project that covers:
   - How to set up and run your service (dependencies, environment, startup command)
   - How to interact with the API -- include **one of**: a Postman collection, Swagger/OpenAPI docs,
     or a set of example `curl` requests covering the main operations
   - Any design decisions worth calling out
   - What you would improve with more time
3. **An `ai-collaboration-summary.md` file** documenting your use of AI tooling (see [AI Collaboration](#ai-collaboration)).

## Language & Frameworks

**Python 3.10+**, **Ruby 3.0+**, and **Elixir 1.14+** are recommended, but choose whichever language you feel most comfortable with.

A web framework is expected -- use whatever you prefer (e.g. FastAPI/Flask, Sinatra/Rails, Phoenix/Plug).
External libraries are also allowed for SSH/SFTP connectivity and YAML/JSON parsing. The core logic
(connection management, adapter design, transfer orchestration) should be your own.

## What We Provide

- **`data/customers.csv`** -- a sample CSV file with 15 rows of customer data
- **`data/products.json`** -- a sample JSON file with 6 product records
- **`docker-compose.yml`** -- spins up a local SFTP server for testing

### Setting Up the SFTP Server

```bash
docker compose up -d
```

This starts an SFTP server on **port 2222** with the following credentials:

| Field    | Value      |
|----------|------------|
| Host     | localhost  |
| Port     | 2222       |
| Username | testuser   |
| Password | testpass   |

Files written to the SFTP server will appear in the `sftp_data/` directory on your local machine.

You can verify the server is running:

```bash
sftp -P 2222 testuser@localhost
```

## What You Build

> **Note on the examples below:** the `curl` requests are *illustrative*. You may design the API
> however you like -- routes, verbs, request/response shapes are up to you. We care about the
> operations being available over HTTP and about clean abstractions underneath, not about matching
> these exact endpoints.

### Phase 1: Connection Management + Local Filesystem Connector

**Goal:** Establish the adapter pattern and build stateful connection management.

**Requirements:**

1. Define a **connector interface** with at minimum these operations:
   - `list` -- list available files in the connection
   - `read` -- read a file's contents from the connection
   - `write` -- write contents to a file in the connection

2. Implement a **local filesystem connector** that operates on a directory path.

3. Build a **connection store** that persists connection configuration (name, type, and type-specific settings). Connections must survive across restarts of your service. You may use SQLite, a JSON file, or any persistent mechanism you choose.

4. Expose the following operations over HTTP (example requests shown -- design the API however you like):

```bash
# Create a local filesystem connection
curl -X POST http://localhost:8080/connections \
  -H 'Content-Type: application/json' \
  -d '{"name": "local_data", "type": "local", "path": "./data"}'

# List files available through a connection
curl http://localhost:8080/connections/local_data/files

# Preview file content and infer schema (for CSV/JSON files)
curl http://localhost:8080/connections/local_data/files/customers.csv/head
```

**Acceptance criteria:**

- A clear connector interface exists
- Local filesystem connector implements the interface
- Connections persist across service restarts
- `list` shows files in the connected directory
- `head` returns the first several rows of a file

### Phase 2: SSH/SFTP Connector + Data Transfer

**Goal:** Prove the adapter pattern works across connection types and build the transfer pipeline.

**Requirements:**

1. Implement an **SFTP connector** that connects to an SSH/SFTP server using stored credentials (host, port, username, password). It should conform to the same connector interface you defined in Phase 1, so the rest of the system can use it without knowing whether the underlying connection is local or remote.

2. Build a **transfer** operation that reads a file from a source connection and writes it to a destination connection. The transfer should be **data-agnostic** -- it moves the file without inspecting or transforming the content.

3. Track **transfer status** including: started/completed timestamps, bytes or rows transferred. This should be observable through the API (e.g. a response body or a status endpoint) rather than only in logs.

4. Expose the following operations over HTTP:

```bash
# Create an SFTP connection
curl -X POST http://localhost:8080/connections \
  -H 'Content-Type: application/json' \
  -d '{"name": "remote_server", "type": "sftp", "host": "localhost",
       "port": 2222, "user": "testuser", "password": "testpass"}'

# List files on the remote server
curl http://localhost:8080/connections/remote_server/files

# Transfer a file from local to SFTP
curl -X POST http://localhost:8080/transfers \
  -H 'Content-Type: application/json' \
  -d '{"source": "local_data", "source_file": "customers.csv",
       "destination": "remote_server", "destination_file": "customers.csv"}'

# Transfer a file from SFTP back to local
curl -X POST http://localhost:8080/transfers \
  -H 'Content-Type: application/json' \
  -d '{"source": "remote_server", "source_file": "customers.csv",
       "destination": "local_data", "destination_file": "customers_from_remote.csv"}'
```

**Acceptance criteria:**

- SFTP connector implements the same interface as the local connector
- Transfers work bidirectionally (local to SFTP, SFTP to local)
- Transfer status is reported via the API (timestamps, size)
- The transfer operation does not contain connector-specific logic (no `if type == "sftp"` branching)
- Adding a new connector type would not require changes to the transfer logic
- Connection failures (wrong credentials, server down) produce clear error responses with appropriate HTTP status codes

### Phase 3: Head Utility + Polish (Optional)

**Goal:** Build a useful schema preview tool and polish error handling.

**Requirements:**

1. Enhance the **head utility** to infer schema from structured file formats:
   - For **CSV**: return column names and inferred types (string, integer, float, boolean, date)
   - For **JSON** (array-of-objects): return field names and inferred types
   - The head utility should work through **any connector** -- local or SFTP

2. Polish **error handling** across the application:
   - Connection creation with invalid parameters gives clear feedback (e.g. `400` with a useful message)
   - SFTP connection failures produce actionable error responses
   - Transferring to/from a non-existent file is handled gracefully (e.g. `404`, not a `500` stack trace)

```bash
# Preview schema of a file on the SFTP server
curl http://localhost:8080/connections/remote_server/files/customers.csv/head
```

**Acceptance criteria:**

- Schema inference produces reasonable types for the provided sample data
- Head utility works through both local and SFTP connections
- Error responses help the caller understand what went wrong and how to fix it, with appropriate status codes

### Bonus (Optional)

These are genuinely optional. Completing any of them signals depth but is not expected:

- Connection healthcheck endpoint (verify credentials are valid without transferring data)
- Streaming/lazy I/O so large files don't need to be fully loaded into memory
- Encryption of stored credentials at rest
- Additional connector types (e.g., S3)
- Unit tests for the connector interface contract
- API documentation served by the application itself (e.g. an OpenAPI/Swagger UI endpoint)

## Guidance

A few suggestions to help you put your best foot forward -- none of these are hard requirements,
but they tend to make for a stronger submission and a better debrief:

- **Be intentional with your git commits.** We like seeing good history -- small, focused commits
  that show how you built the project incrementally tell us far more than a single squashed commit.
  Clear commit messages are a plus.
- **Annotate your code where it adds context.** Feel free to leave comments explaining a non-obvious
  decision, calling out a `TODO`, or noting an edge case you're aware of but deliberately chose not to
  handle. We'd rather see "I know about this, here's why I skipped it" than wonder whether you missed it.
- **Scope deliberately, and say what you cut.** It's fine to leave things unfinished given the time box.
  Note the tradeoffs you made and what you'd do with more time in your README -- thoughtful scoping is a
  positive signal, not a negative one.
- **Optimize for explainability.** Submit code you can walk us through and reason about. Clean
  abstractions you understand beat clever ones you can't defend.

## Submission

- Submit as a Git repository (public or private -- if private, grant access to the reviewer)
- Include the deliverables listed in the [Deliverables](#deliverables) section above
- Commit history matters -- we'd like to see how you built it incrementally, not as a single commit

## What We Value

In priority order:

1. **Clean abstractions** -- a well-designed connector interface matters more than feature count
2. **Working code** -- we will run your service against the provided SFTP server
3. **Stateful connections** -- connections should persist and be reusable
4. **Thoughtful error handling** -- clear responses over silent failures or raw stack traces
5. **Code organization** -- sensible file/module structure for a growing project

## Rules

- Open book: use documentation, Stack Overflow, language references
- AI tooling is allowed and expected. Please document your process (see the AI Collaboration section below).
- External libraries are fine for the web framework, SSH/SFTP, and parsing

## AI Collaboration

With the advent of agentic coding, we expect our engineers to leverage AI tooling and workflows, so using these for this take-home is allowed and encouraged.
However, we also expect that the code you submit is code you can explain in-depth, including the reasoning behind architectural decisions.

We are **not** grading on whether or how much AI you used; we are grading on the quality of the code and your ability to reason about it.

As part of your submission, **paste the prompt below into the AI tool you used and save its response in a file called `ai-collaboration-summary.md`** at the root of your project:

> You are a collaborator on this repository. Summarize how you assisted the engineer in building this
> project. Cover: the tools and workflow used, which parts of the architecture and code you contributed,
> and where the engineer directed, corrected, or overrode you. Include 2-3 concrete examples of prompts
> you were given and the outputs you produced. Be honest about what was AI-generated versus
> human-authored. Keep the summary to one page (500 words or less).

If you used multiple tools, run the prompt against your primary one and note the others in the file. If you did not use AI at all, add a short note in the file saying so and briefly describe your process instead.
