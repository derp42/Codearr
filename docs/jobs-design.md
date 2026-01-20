# Jobs & Transcode Tree Design

This document proposes how Codarr jobs and the transcode flow designer should work.

## Goals

- Keep the node responsible for execution logic (healthcheck + transcode).
- Keep the server responsible for job orchestration, UI, and flow definitions.
- Support a node-based, drag-and-drop tree (React Flow style).
- Make trees represent a decision tree that can build FFmpeg commands and validate output.

## Job Types

### 1) Healthcheck

Purpose: validate the file is readable and has the expected media streams/metadata.

Suggested checks:

- File exists and is readable.
- Container is recognized (by ffprobe).
- Duration and stream counts are sane.
- Track whether initial metadata is captured (container, codec, size).

Output (per job):

- Updates file row with `initial_*` metadata.
- Emits logs for each validation step.
- If failed, mark job error and file `health_failed`.

### 2) Transcode

Purpose: execute a tree definition that decides *how* to transcode and validates results.

Input: JSON tree definition (see schema below).

Output: final file metadata, replace original (optional), or fail.


## Data Model Additions (Server)

The server should persist reusable tree templates and link files/libraries to them.

### Tables

Add tables:

- `trees` — saved tree templates
- `tree_versions` — immutable version history
- `library_tree_map` — which tree is assigned to a library (optional)

Example schema (SQLite):

```sql
CREATE TABLE IF NOT EXISTS trees (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tree_versions (
  id TEXT PRIMARY KEY,
  tree_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  graph_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(tree_id) REFERENCES trees(id)
);

CREATE TABLE IF NOT EXISTS library_tree_map (
  library_id TEXT NOT NULL,
  tree_id TEXT NOT NULL,
  tree_version INTEGER NOT NULL,
  PRIMARY KEY(library_id),
  FOREIGN KEY(library_id) REFERENCES libraries(id),
  FOREIGN KEY(tree_id) REFERENCES trees(id)
);
```

### Job Payload

When a transcode job is enqueued, include a `transcode_payload` JSON column in `jobs` with:

- `tree_id`
- `tree_version`
- `graph_json` (denormalized copy for history)
- `context` (optional, e.g., library ID, user notes)


## Flow Graph Schema (Draft)

Each tree is a directed graph of nodes with typed inputs/outputs. It is stored as JSON and visualized with React Flow.

### Top-level

```json
{
  "version": 1,
  "nodes": [/* node list */],
  "edges": [/* edge list */]
}
```

### Node Types

All nodes share:

```json
{
  "id": "node-1",
  "type": "input_file|check_container|check_video_codec|check_audio_codec|build_ffmpeg|validate_size|replace_original|fail_job",
  "name": "Input File",
  "data": { /* type-specific config */ }
}
```

#### Input File

- No inputs.
- Provides file metadata (path, size, container, codecs, streams).

```json
{
  "type": "input_file",
  "data": { "label": "Input" }
}
```

#### Check Container

- Accepts `container` string.
- Branches: `match` or `no_match`.

```json
{
  "type": "check_container",
  "data": { "allowed": ["mkv", "mp4"] }
}
```

#### Check Video Codec

- Accepts `videoCodec`.
- Branches: `match` or `no_match`.

```json
{
  "type": "check_video_codec",
  "data": { "allowed": ["hevc", "h264"] }
}
```

#### Check Audio Codec

- Accepts `audioCodec` (primary or any).
- Branches: `match` or `no_match`.

```json
{
  "type": "check_audio_codec",
  "data": { "allowed": ["aac", "ac3", "eac3"] }
}
```

#### Build FFmpeg

- Emits an FFmpeg command definition.

```json
{
  "type": "build_ffmpeg",
  "data": {
    "container": "mp4",
    "video": { "codec": "hevc", "preset": "medium", "crf": 22 },
    "audio": { "codec": "aac", "bitrateKbps": 256 },
    "extraArgs": ["-map", "0"]
  }
}
```

#### Validate Size

- Ensures output is within allowed size thresholds.
- Branches: `ok` or `fail`.

```json
{
  "type": "validate_size",
  "data": { "maxRatio": 1.05, "minRatio": 0.7 }
}
```

#### Replace Original

- Moves new output into place and updates file metadata.

```json
{
  "type": "replace_original",
  "data": { "keepBackup": true, "backupSuffix": ".bak" }
}
```

#### Fail Job

- Terminal node for a failed execution.

```json
{
  "type": "fail_job",
  "data": { "reason": "Size check failed" }
}
```


### Edges

Edges connect node outputs. Each edge can include a `sourceHandle` to represent branching outputs.

```json
{
  "id": "edge-1",
  "source": "node-1",
  "target": "node-2",
  "sourceHandle": "match"
}
```


## Execution Model (Node)

The node resolves the tree and executes it in order, using a small engine:

1. Load job payload (tree graph).
2. Build a graph index and validate it (single input, no cycles, all terminals reachable).
3. Execute from `input_file` node.
4. Each node returns:
   - `outputs`: values to pass to next nodes
   - `next`: next edge handle to follow

The node will also emit structured logs per node:

```json
{"ts":"2026-01-19T12:00:00Z","stage":"build_ffmpeg","message":"Using hevc preset=medium crf=22"}
```


## UI (React Flow)

Initial node palette:

- Input File
- Check Container
- Check Video Codec
- Check Audio Codec
- Build FFmpeg
- Validate Size
- Replace Original
- Fail Job

UI responsibilities:

- Build a directed graph with constraints (one input, explicit branching outputs).
- Validate before save.
- Serialize to the tree schema and POST to server.


## Suggested Next Steps

1. Implement server flow storage APIs.
2. Add `transcode_payload` in `jobs` table.
3. Build a lightweight flow validator (server-side and node-side).
4. Implement node execution engine for the initial nodes.
5. Add UI view for "Transcode Trees" with React Flow and a basic node palette.
