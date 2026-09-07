# Changelog

All notable changes to `skcoord` are documented here.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning: [SemVer](https://semver.org/spec/v2.0.0.html). The git tag IS the
version (setuptools-scm); a push to `main` cuts the next patch tag (see
`.github/workflows/publish.yml`), and pushing a `v*` tag directly releases it.

## [Unreleased]

### Added

- A constrained exact-head GitHub review broker now requires a terminal local
  CI-parity receipt before submitting a review. The broker verifies the exact
  canonical receipt bytes, atomically reserves each request across processes,
  requires completed-success checks, and never retries an uncertain review
  side effect. Card `dec5a948`.

- Board scan-cost helpers: `Board.prune_stale_locks`, live-only defaults for
  `generate_board_md` / `get_briefing_*` (`include_done`), and CardStore folds
  that stash `_board_updated_at` for archive aging. Cards b0a0d002/b0a0d003/b0a0d005.

- Hash-chain event log integrity: `append_event` now computes `prev_hash`
  (SHA-256 of the preceding line in the same writer file) and `_read_events`
  verifies the chain at fold time. Tamper or truncation raises `ValueError`
  with the exact break point. Backward compatible: legacy unchained events
  pass (the chain starts at the first chained event). Card 887643f5.


### Changed

- Docs: SOP.md now grounds the CHI census and the shared home path in
  `SITE_AND_HOST_NAMING_STANDARD`. The estate boundary is the control plane (one
  `~/.skcapstone`, one Syncthing ring, one trust root, one operator), not the
  tailnet, which is why unrelated tailnet devices are correctly out of census
  scope; and a CI is never renamed to adopt the standard, because a CI id is a key
  whose rename deletes the event history that current state is folded from.
  `docs/cmdb-workflow-proposals.md` P8 gets the same boundary: canonicalisation
  collapses addresses, never respells names. The `chioc09` alias of canonical
  `chiap09` and every other host reference are statements of fact and are
  unchanged.

### Fixed

- Atomic create-and-claim retries now require exact equality across every
  immutable card field before accepting an existing ID, including concurrent
  same-owner requests with different payloads. An exact replay also fails
  read-only when its original claim is no longer current. Card c1a1c003.

- Bounded CardStore estate scans now isolate an unreadable card instead of
  aborting the whole page, while direct folds remain strict and source event
  bytes remain untouched. Card c0bfab02.

- `archive_done_tasks` ages done cards from the legacy+CardStore union and
  prefers CardStore `updated_at` as completion time, while `age_stale_open`
  still trusts authoritative legacy claim status and skips `human-gate` /
  `[HUMAN]` cards.

- Card daeac75b refreshes the exact guarded claim-conflict release candidate
  against current main while preserving authoritative owner state and requiring
  the exact losing owner and claim revision before mutation.

- Completed agent work now projects availability as `idle`, preserves `offline`,
  and normalizes malformed legacy `state=completed` input before serialization.

- Card fc2d87bf makes void terminal at the mutation boundary: lifecycle moves
  now refuse any card carrying a void audit event, preventing later bulk moves
  from overriding the archive event and returning retired work to the board.

- `lifecycle_reassessment` now reads supersession from the EVIDENCE store, not
  only from the structure store. `superseded_by` is normally recorded as an
  evidence link, and none of those were visible, so a superseded card stayed
  assignable alongside its own successor. This is the same two-store defect
  already documented in this module for BLOCKED verdicts, fixed there and never
  carried across. Measured 2026-08-28: `c91a7504` carried `superseded_by`
  `2209f7fe` since 2026-08-23 and was never excluded; detection rises from 7
  superseded cards to 28.
- Evidence-derived supersession can no longer retire a HUMAN approval gate.
  `superseded_by` in the evidence store is free-form and any worker can write
  one, so honouring it unguarded let a machine card discharge an approval only a
  person can give. Without the guard this change retired gate `36afc5e8` in
  favour of machine task `6dd21df9`. Structure-store supersession is a
  deliberate authored act and still applies.

- `lifecycle_reassessment` now isolates a card whose `core.json` cannot be
  parsed instead of raising, and skips an event line that is not yet whole.
  `~/.skcapstone` is a single Syncthing folder, so a file written on one host
  is observable mid-write on the others; `assess` raising on that aborted the
  fleet rotation, which exits non-zero when the assessment fails, on every host
  simultaneously. Measured 2026-08-27: all five hosts stopped launching on a
  `core.json` that was two bytes long at the instant it was read, and nothing
  was actually corrupt. Damaged cards are reported in a new `unreadable_cards`
  class and added to `excluded_card_ids`, so the failure mode is to withhold
  one card rather than to stop all work.

- Datastore discovery now normalizes container identity against restart tokens, the
  same `_stable_container_name` rule PR 50 added for the service collector. The
  SKLegal job launcher names PostgreSQL containers `<subject>-<pid>-<8 hex run
  token>`, and `collect_datastores` embedded the raw name, so every restart of a
  job database minted a new undeclared datastore CI (47 such CIs were created after
  the PR 50 merge, most recently 2026-08-31). Identity is now derived from the
  stable subject; the launcher PID and restart token are preserved as attributes
  and the raw name is kept as an alias, exactly like `collect_docker_containers`.
  Regression tests cover the restart pair and the no-suffix negative control
  (card 151323cb, follows 72f49960).

### Added

- Card b426f8e6 adds a versioned scheduler-truth contract with separate
  structural and runtime reasons, exclusive primary counts, diagnostic facets,
  canonical legacy aliases, a read-only JSON interface, and operator actions.

- Card creation now enforces review-chain governance at `CardStore.create`, so
  coordination CLI and MCP callers share the same fail-closed policy. Live
  review or repair duplicates for one parent and class are refused with the
  existing card ID, review depth is capped at one review plus one re-review,
  and only the exact `human-override` label bypasses those checks. Refused
  mirrored creates no longer leave a legacy task projection behind. CapAuth is
  temporarily bounded below 0.3.10 because that release makes the existing
  operator-session currentness contract fail closed in clean installations.
- Schemas and validators for the two coordination stores, derived from live data
  rather than intent: `schemas/itil-record.v1.schema.json` and
  `schemas/itil-event.v1.schema.json` (all 318 records and 1702 events scanned,
  every event validates), and a controlled `link_key` vocabulary for the evidence
  store (`schemas/evidence_vocab.py`, `schemas/card-event.v1.schema.json`,
  `schemas/evidence-key-map.v1.json`) with `schemas/validate_itil.py` and
  `schemas/validate_card_events.py` (#47).

  The evidence store used 3674 distinct `link_key` values across 19408 link
  events, so readers matching a key literally missed most of what was recorded.
  `human_approval` alone had 26 spellings: a check reading only the canonical key
  found 41 of 173 recorded approvals. Folding never invents a concept; a key that
  cannot be confidently folded stays uncontrolled and is reported rather than
  reinterpreted.

- `dead_worker_claims` class in the lifecycle reassessment report, covering a
  claim held by an ephemeral worker that produced no evidence at all. Named
  agents are excluded by owner pattern, never by age (#48).

### Fixed

- Kanban and coordination status aggregation now degrade one failed CardStore
  fold into an explicit `UNREADABLE` card with its identifier, source, and
  reason. Readable cards remain visible, while direct folds, malformed event
  reads, parity, and export remain strict and fail closed (card `5f809dfe`,
  defect family `00bb0a6a`).

- Stale-claim detection now joins the evidence store. It previously scanned only
  `cards/<id>/events/*.jsonl` looking for verdicts that are written to
  `coordination/card_events/*.jsonl`, so it reported `stale_claims: 0` against a
  live store holding cards claimed for days with recorded BLOCKED verdicts. Its
  test passed because the test wrote a synthetic verdict onto a card event, a
  shape production never produces. Verdict matching also required exact equality
  with `BLOCKED` while live verdicts are qualified (`BLOCKED_FAIL_CLOSED`), and
  link keys were compared unfolded (#48).

### Fixed

- Legacy task fields that are absent now remain unknown during parity checks
  instead of being projected as disagreements. In particular, status-less
  immutable birth records no longer make complete CardStore chains appear
  stale or inflate the parity open-count alert (card `9ccc42ec`).

- Incident folds now apply append-only `assignment` events to `managed_by`,
  and `ITILManager.update_incident` can append those events without rewriting
  immutable incident cores or changing creation provenance (card `aece9475`).

- CardStore mutations now reject targets without a foldable immutable core,
  legacy migration preserves acceptance criteria with the other birth facts,
  and every board or CardStore entry point rejects a `coordination/`
  subdirectory passed as the shared home before it can strand nested events.
  Describe and link overlay writes also require that core before opening an
  append-only overlay file (card `0146e817`).

- Same-day recurring deterministic service-health incidents now reopen their
  existing append-only record instead of remaining resolved or creating a
  duplicate incident (card `5b57816b`).

- Legacy task views now fold append-only acceptance criteria amendments from
  CardStore in every rollback selector, keep birth criteria only for tasks
  never mirrored into CardStore, and fail closed on malformed or unreadable
  known-card state. Criteria drift is now a gating parity mismatch, and task
  files plus `core.json` remain immutable (card d9b08c7a).

- `atomic_write_text` closes its temporary descriptor before replacing the
  target and closes both temporary and parent-directory descriptors on every
  failure path. This prevents the append-only board's safe writes from leaking
  one file descriptor per successful mutation in long-running workers (card
  54cd56f2).

- `Board.claim_task(..., force=True)` no longer bypasses dependency gates.
  The compatibility flag remains accepted, but incomplete, unknown, review,
  and human dependencies now fail closed and list every blocking ID in every
  supported CardStore mode (card 54cd56f2).

- A change's `[ITIL:<id>] Implement: <title>` GTD next-action is now emitted at
  most once (card a7e3ca15, follow-on to the entry below). A change approved by
  CAB vote already folds `approved` before `update_change` runs, so re-issuing
  `new_status="approved"` (the CLI run twice, a retried MCP call) cleared the
  fold check while the fold moved nothing, landing a SECOND high-priority
  implement task on the operator's board for one approval. The guard reads
  `gtd_item_ids`, which is folded from `gtd_link` events written only after an
  emission that actually happened, so a REFUSED approval leaves it empty and
  cannot be used to suppress the genuine approval's task later.

- `ITILManager.update_change` now fires the approval and deploy side effects
  from the FOLD RESULT instead of the requested status (card a7e3ca15). The
  CAB bypass guard added in 941570f correctly refuses a raw `status` event
  that tries to grant approval, but the `itil.change.approved` publish and the
  high-priority `[ITIL:<id>] Implement: <title>` GTD next-action were emitted
  before the fold ran, keyed only on `new_status == "approved"`. A blocked
  self-approval therefore left the record `proposed` while announcing an
  approval to every bus consumer and landing an implement task on the
  operator's board -- a reader of the bus or of the GTD board could not tell a
  real approval from a refused one. The same reordering covers
  `itil.change.deployed`. Legitimate approvals (a qualifying CAB vote, the
  standard / auto-normal derivation) publish and emit exactly as before; the
  fold guard itself is unchanged.

- A `.timer` no longer emits a `depends_on` edge to its own same-named
  `.service`. Both fold to one `ci-service-<base>` CI, so systemd's ordinary
  timer-to-service dependency produced a self edge; a self edge fails
  validation, and `reconcile --apply` refuses to run while any validation
  failure is present, which silently blocked every apply on nodes running such
  a timer (card 0bc46220).

### Changed

- Split the CMDB discovery implementation into focused collector,
  reconciliation, scan, drift, and shared-model modules while preserving
  `skcoord.discovery` as the compatibility import surface.

### Added

- Added a durable bounded owner-policy backend for authorized CardStore reads.
  It loads exact immutable entries from owner-controlled storage, rejects
  malformed or unsafe files, and suppresses results when policy identity,
  revision, or validity changes during a protected read (card `19acf874`).

- Added a frozen SKCoord owner-policy index and provider that binds only an
  exact policy-selected CardStore identifier set to the current attributable
  CapAuth decision before the authorized snapshot reader can access records
  (card `cf0e34cd`).

- Added a pure bounded authorized CardStore snapshot reader that validates a
  fully bound CapAuth and owner-policy decision before folding only the
  policy-visible identifier set, emits only allowlisted record and graph
  evidence, and returns one constant no-value result on every authorization
  failure (card `a110cadc`).

- Added immutable, persona-neutral Portfolio Steward contracts and a pure
  shadow-only readiness evaluator with deterministic ordering, explicit
  abstention, exact content bindings, and independent-review completion gates
  (card `7efc76c0`). The slice has no board, authorization, model, network,
  execution, or mutation integration.

- Added `Board.record_success`, the write half of cross-run success memory
  (card 506782a4, S9). Sibling of `record_attempt`, writing distilled
  terminal-PASS entries to `meta.autopilot.successes[]` -- a key
  `clear_attempts` never touches, so a success recorded on the same pass
  that triggers `clear_attempts` survives it instead of being wiped by the
  event that created it. Mirrors `record_attempt`'s (run_id, outcome)
  idempotency and 10-entry corruption cap; the reader side (skharness)
  decides how much of it reaches a prompt.
- Added secret-free Syncthing discovery for each scanned node, including
  service health, version and schema, folder state, pending work, connected
  devices, and governed service relationships.

- Added scheduled CMDB reconciliation policy helpers with validated,
  versioned configuration, a nonblocking application lease, configurable
  drift and collection-failure thresholds, deduplicated ITIL incident routing,
  and bounded checksummed run-artifact retention.

- Added secret-free CapAuth identity-estate discovery for actual and alternate
  user homes, canonical and compatibility roots, Syncthing folders, signer
  fingerprints and roles, material placement, verification age, and requested
  identity drift findings.

- Added a fail-closed CMDB write guard that rejects secret-looking attribute
  keys, including nested mappings, before a write-once core or append-only event
  can be created. Matching is case-insensitive and substring-based; the shared
  matcher also drives reconciliation-artifact redaction.

- **Kanban lifecycle and agent projection reconciliation.** The authoritative
  event-sourced card can reach Review or Done while an interrupted caller leaves
  `agents/<name>.json` reporting the card as current work. `audit_lifecycle()`
  now reports those disagreements without writing, and `repair_lifecycle()`
  explicitly converges only the mutable agent projection. Review retains the
  accountable owner but clears active execution, Done clears claims while
  preserving completion history, and reopen removes stale completion state.
  Recent orphan work or multiple recent active owners stop the repair rather
  than guessing. Every repair appends a conflict-free JSONL receipt under
  `coordination/reconciliation/`; a second repair is idempotent.

- Added fail-closed CMDB evidence validation, explicit relationship deltas,
  secret-redaction findings, stale/retirement plan fields, checksum-verified
  artifact reads, and concurrent-writer coverage for the supported plan/apply
  workflow (`e57ef91a`). The legacy seed method is now a versioned compatibility
  bridge over declared discovery rather than a hard-coded three-host inventory.

- Expanded observed CMDB discovery from four collectors to nine. Cross-node
  scans now cover user/system cron entries (with command arguments redacted),
  non-loopback network interfaces, persistent mounts and database containers,
  remote agent homes, Ollama endpoint/model health, Podman containers, and
  Docker/Podman Compose labels in addition to host, systemd, container, and
  listening-port evidence. Collector totals remain part of the checksummed
  shadow scope, so a release cannot silently claim the old coverage contract.
  Each target now also records command attempt/success/unavailable counts and a
  complete/partial/unavailable collector status without retaining command text
  or output, making WSL, permission, and missing-tool gaps explicit. A fully
  unavailable collector makes its target and scan incomplete; a successful
  fallback with unavailable optional commands remains explicit `partial`.

- Defined the operator-reviewed CHI fleet Node objects as the authoritative
  CMDB discovery census, including identity/source precedence, the
  `chiap09`/`chioc09` alias decision, `chipv05` and Windows/WSL handling,
  deterministic CI identity, four-hour staleness, three-complete-pass
  retirement, relationship vocabulary, and no-secret credential metadata.

### Fixed

- Fixed three CMDB drift-matcher false-positive classes responsible for 23 of
  27 "high severity" findings in the 2026-08-23 audit: `collect_cron_jobs` now
  parses `sk-cron-run.sh <name>` wrapper invocations (fixing an inline
  `VAR=value` parsing bug that silently dropped those lines) and carries the
  declared job name as an alias; Operatorapp-kind CIs (CLI tools, not
  daemons) are now preserved with a `fleet_kind`/`cli` marker and exempted
  from `drift()`'s declared-not-observed running-unit check; and
  `collect_fleet_objects`/`collect_registry` now carry `spec.unit` and
  registry `pid_file` stems as declared aliases. `drift()` now matches on the
  full alias set (`_service_keys()`) instead of only `.name`.
- Limited automatic CMDB incident routing to CIs carrying the explicit
  `alert-on-drift` tag, preventing generic inventory drift from creating
  unreviewed incident floods.

- Reverted the stale coordination rewrite in `c5731be`, restoring canonical
  agent projection validation, transactional lifecycle repair, ownerless
  ready/doing claims, and fail-closed dependency enforcement. The existing
  claim API already accepted `force=True`, so no compatibility behavior was
  lost.

- Fold acceptance-criteria amendments into the authoritative CardStore
  projection and task views while keeping `core.json` birth facts immutable.
  The legacy kill switch now preserves criteria on projected cards, and
  rollback export writes the current folded criteria for store-born tasks.

- Preserved the terminal change lifecycle across the CAB provenance upgrade
  (docs/adr/0002). Votes recorded before authenticated
  `subject_role`/`subject_fingerprint`/`authorization_id` fields existed
  demoted already-closed historical changes (chg-a76c0aee, chg-1dc7aa09) back
  to `reviewing` at fold time. An unprovenanced approval vote decided before
  the fixed past cutoff `_LEGACY_CAB_PROVENANCE_CUTOFF` is now honored as a
  historical human approval — a schema-derived, fold-time-only rule that
  rewrites no history and fabricates no vote. The raw-status CAB bypass guard
  stays fail-closed: no vote written after the cutoff can satisfy the legacy
  clause, and new changes still require authenticated provenance.

- Stopped promoting every observed host interface address into a CMDB identity
  alias. Reused container-bridge addresses caused a ten-node CHI scan to merge
  all hosts into one CI; addresses remain provenance-rich attributes while
  governed names and explicit aliases control identity reconciliation.

- Added a validated optional SSH port to the skvault metadata adapter and
  strict SSH runner, allowing WSL targets on port 2222 without inline options
  or reliance on ambient SSH configuration.

- Separated the canonical target identity from an optional validated SSH
  transport hostname so dual Windows/WSL nodes remain one CI while collection
  reaches the reviewed WSL endpoint.

- Publish the auto-tagged release to PyPI in the same GitHub Actions run. Tags
  pushed by the workflow's `GITHUB_TOKEN` do not trigger a second workflow, so
  the former tag-only publish guard produced green builds and tags through
  `v0.1.15` while PyPI remained at `0.1.8`.

- Restored the missing `Board.set_grade()` write/return after adding optional
  session and node provenance. The interrupted edit previously built the grade
  document but never persisted it, breaking coordination grading at runtime.

- Made bounded CMDB deadline handling portable across supported Python
  versions by catching `concurrent.futures.TimeoutError` explicitly; Python
  3.10 no longer leaks a deadline exception instead of returning an incomplete
  fail-closed scan result.

- Recorded the verified three-shadow CMDB scope, authenticated Chef approval,
  exact ATLAS network unit contract, freeze/report-only cutover sequence, and
  rollback acceptance checks in the operational rollout SOP.

### Fixed
- **The release model in `publish.yml` matches what the repo documents (CMDB-7).**
  PR #4 switched the trigger to tags-only but left the `tag` job in place — and
  that job only runs on a push to `main` — so the documented "a push to the
  default branch cuts the next patch tag" model (pyproject, this file) was dead
  code: merging to main no longer released anything, which is how the #14 merge
  sat unpublished on 2026-08-17. The main branch is back in the trigger, and
  `pypi-publish` now runs only on the tag-push path, so a main push cuts the
  tag and verifies the build (twine check) while exactly one run — the tag
  push it creates, or a manually pushed tag — uploads to PyPI. No more dead
  job, no more double-upload race, and the docs are true again.
- **CMDB reconcile now derives CIStatus from observed state.** Discovery-owned
  CIs could read `degraded` on the headline status field while their own
  `active_state` attribute said `active`, because `seed_from_inventory` derived
  status from open ITIL incident severity and `reconcile` only wrote attributes
  and relationships. Deployed via the same path as the incident version.

  Precedence rule (CMDB-6): observed systemd state is authoritative for
  discovery-owned CIs. `active_state=active` reads operational, `failed` reads
  down; inactive/activating are ambiguous (oneshots, timers), so no status is
  forced and any existing value is left alone. ITIL incident severity stays
  informational via `impact_analysis` and never overrides an observed state on
  the CIStatus field. A manually retired CI is never un-retired by reconcile.
- **`reconcile_from_legacy()` no longer un-completes work.** It converges the
  store ONTO legacy, which was safe before the Phase-4 read cutover and is not
  safe now: the board is served FROM the store, so legacy is a projection that
  lags, and a card completed in the store but not yet reflected in legacy is
  indistinguishable from real drift. Converging such a card moved it out of
  `done`, and the parity gate then went green *because* the completion had been
  destroyed. Observed live on card `b24c71b5` (2026-08-17), whose store held
  `claim -> move -> complete` while legacy still said `ready`/`lumina`. Not
  hypothetical and not manual: the parity soak runs `reconcile --apply` every
  four hours, and card `70dad715` carries reconcile-written events dragging it
  `ready -> backlog` twice across two days.

  A card whose store state is `done` is now skipped **whole** and returned in
  `skipped_uncomplete`, rather than partially converged. Rewriting its owner
  while leaving its status alone would leave the card in a state neither side
  ever held and still would not converge parity. `allow_uncomplete=True` opts
  back in.

  Deliberately interim. Which side is authoritative for status/owner after the
  read cutover is a design decision (card `be8d5561`); un-completing work is not
  something a drift-repair tool should do silently whichever way that lands.

### Added
- **`skcoord.discovery`: CMDB discovery over declared and observed fleet state.**
  `cmdb.seed_from_inventory()` hardcoded three hostnames and scraped the rest of
  its service list out of ITIL incident `affected_services`, so the CMDB could
  only ever describe the fleet someone had already typed into it: 48 CIs, none
  from a scan. The new collectors read the sources we actually keep (fleet
  objects, the service registry, agent homes) plus real machine state (systemd
  services and timers, docker containers, listening sockets). A first live run
  on `.158` found 442 CIs against the 48 stored.

  Every `DiscoveredCI` records whether a fact was **declared** (a spec claims
  it) or **observed** (a machine answered), because a CMDB fed only
  declarations cannot tell you it is wrong. `drift()` is the report that only
  exists because the two are kept apart: `declared_not_observed`,
  `observed_not_declared`, `stored_not_discovered`.

  Machine access goes through a `CommandRunner` (local or ssh), so the same
  collectors run against a remote node and against canned output in a test.
  `reconcile()` is additive only: it creates, updates attributes and
  relationships, and reports CIs it no longer sees as orphans. It never
  deletes, because a collector that silently failed would otherwise erase
  inventory. Decision recorded in `adr/ADR-002-cmdb-canonical-store.md`.

### Fixed
- **Ephemeral listening ports accreted as permanent CIs.** The port collector
  recorded every socket in LISTEN, including the random high ports that RPC,
  mDNS, tailscale and short-lived servers bind. The CMDB is append-only and the
  reconcile cron runs every three hours, so each reboot's fresh set of high
  ports would be created forever while the previous set turned into permanent
  orphans. Observed on `.158`: 15 such CIs after a single day, one already
  orphaned. Ports inside the host's ephemeral range are now skipped, and the
  range is read from the host being scanned (`ip_local_port_range`) rather than
  hardcoded, so a tuned node and a remote ssh scan both use the right bounds.
  The skipped count is logged, because a host silently reporting fewer ports
  than it has is the same failure as reporting none.
- **Drift accused healthy services, three ways.** Getting the report from 372
  findings down to 87 took three fixes, all of which had it crying wolf:
  `merge()` collapsed declared and observed into one flag, so a service that
  was both correctly declared *and* running came out looking undocumented;
  only `.service` units were collected, so every fleet cronjob (a `.timer`) and
  every `runtime: docker` service read as missing; and
  `systemctl show '*.service'` matched only 78 of 211 loaded units, because the
  glob expands against active units, leaving everything inactive-but-loaded
  unclassified and reported. Units are now named explicitly in the lookup,
  classified by `FragmentPath` so distro units are excluded, and `not-found`
  units (referenced by a dependency, never installed) are dropped as the
  dangling references they are. Origin stays three-valued: a failed lookup
  marks a unit `unknown` and still reports it, so a broken lookup shows up as
  noise rather than as a suspiciously clean report.
- **Full SK_REPO_DOC_STANDARD doc set.** `SOP.md` (9 sections, architecture
  diagram, and an executed `docs-evidence` block of 10 hermetic drift checks),
  `CONTRIBUTING.md`, and `CODE_OF_CONDUCT.md`, plus a `docs-check` CI gate
  (`.github/workflows/docs-check.yml`, tiers 1 and 2) wired to the shared
  sk-standards validator.
- `secret-scan` CI gate running the **gitleaks binary** over the full history.
  Not `gitleaks-action`: that wrapper requires a paid licence for
  organization-owned repos and exits before scanning anything, which produced a
  permanently red check elsewhere in the fleet that scanned zero bytes.

### Fixed
- **README described `SKCOORD_CARD_STORE` backwards.** It read as an opt-in gate
  ("gated by `SKCOORD_CARD_STORE`"), but `card_store_read_enabled()` /
  `card_store_write_enabled()` have been default-ON since Phase 4e: the store is
  disabled only by an explicit `0` / `off` / `false` / `no`. The README now
  describes it as the kill switch it is, and states that the CardStore root is
  `~/.skcapstone/cards/`, a sibling of `coordination/` rather than a child.

## [0.1.5] - 2026-08-14

### Added
- **Cross-run failure memory (storage half).** `Board.record_attempt()` and
  `Board.clear_attempts()` maintain `meta.autopilot.attempts[]`, a sibling of
  `scores[]` / `edits[]`.

  A terminal non-pass of an autopilot build previously died with the run:
  in-run feedback never left `run()`, so the next run of the same card rebuilt
  into the identical wall. The card now carries one distilled line per terminal
  failure, which skharness reads back at run start.

  - `record_attempt(task_id, run_id, round, outcome, tried, why_failed, replacement_hint="")`
    is **idempotent on `(run_id, outcome)`**: a retried finalize or a
    crash-resume replaces the entry in place instead of double-counting one
    failure. Storage is capped at 10 entries as a corruption guard, which is
    distinct from the reader's context policy.
  - `clear_attempts(task_id) -> list[dict]` wipes the array and **returns** the
    removed entries rather than writing a journal. skcoord has no journal code
    and the autopilot run journal is skharness-owned, so the caller archives
    them. This preserves the "skcoord stores facts, skharness decides" split.

  Additive-only: one new optional array written exclusively through
  `_write_task_raw`, so it cannot clobber sibling keys and needs no migration
  across the Syncthing fleet. Cards predating the field behave exactly as before.

  Spec: `skharness/docs/specs/2026-08-14-skharness-failure-memory.md`.
  Tests: `tests/test_failure_memory.py` (9 cases, TDD).

## [0.1.4] and earlier

Not retrofitted. See `git log` and the release tags for history predating this
changelog.

[Unreleased]: https://github.com/smilinTux/skcoord/compare/v0.1.5...HEAD
[0.1.5]: https://github.com/smilinTux/skcoord/releases/tag/v0.1.5
