# Security

This is the threat model: every kind of hostile input, the cap or check that
handles it, and the test that proves it. The guiding rule is that every parse
and every peer-supplied value is bounded, and anything that fails validation
is rejected (or, for peers, strikes/bans) instead of being trusted.

## Metainfo (.torrent) and magnets — local or fetched

| Hostile input | Cap / check | Test |
| --- | --- | --- |
| Nested bencode bombs | Nesting depth limit → `BencodeError::MaxDepthExceeded` | `bencode.rs` depth tests |
| Oversized / truncated bencode strings | String length must fit the remaining input → `StringTooLong`; trailing bytes rejected | `bencode.rs` tests |
| Duplicate dictionary keys | Rejected (`DuplicateKey`) | `bencode.rs` tests |
| Piece count / piece length mismatch | `PieceCountMismatch`, `InvalidPieceLength`, pieces must be 20-byte multiples | `metainfo.rs` tests (`rejects_wrong_piece_count`, `rejects_zero_piece_length`, `rejects_bad_pieces_length`) |
| Path traversal (`.`, `..`, `/`, `\`, NUL) | Rejected per component on every platform | `paths.rs` `traversal_rules_apply_on_every_platform`; `metainfo.rs` `rejects_unsafe_names`/`rejects_unsafe_path_segments` |
| Windows-reserved names, trailing dots/spaces, `< > : " \| ? *`, control chars, >255 UTF-16 components | Rejected when validating for Windows; the rules are pure functions tested on every OS | `paths.rs` reserved-name/forbidden-character/length tests; `metainfo.rs` platform-conditional tests |
| Case-colliding files (`A.txt` + `a.txt`), duplicate paths, file-vs-directory clashes | `ConflictingPaths` typed error; the torrent fails instead of corrupting data | `paths.rs` collision tests; `metainfo.rs` `case_colliding_files_fail_the_torrent_on_windows`, `identical_file_paths_fail_the_torrent_on_every_platform` |
| File lengths overflowing the total | `LengthOverflow` | `metainfo.rs` tests |
| Oversized .torrent via the desktop app | The Tauri `add_torrent` command rejects anything above 10 MiB and non-regular files | `src-tauri/src/main.rs` (`MAX_TORRENT_FILE_BYTES`) |
| Magnet with no peer source | Surfaced as an error in the stats instead of silently waiting | `engine/mod.rs` (`spawn_from_magnet`) |

## Tracker responses — remote

| Hostile input | Cap / check | Test |
| --- | --- | --- |
| Huge response bodies | Content-Length and streamed-size cap `MAX_TRACKER_RESPONSE_BYTES` → `ResponseTooLarge` | `tracker.rs` + `tracker_tests.rs` (`spawn_oversized_content_length_tracker` in engine tests) |
| Chunked endless responses | The streamed read enforces the same cap mid-body | `tracker_tests.rs` |
| Failure reasons, HTTP error codes | Parsed into `TrackerError::Failure` / `HttpStatus`; trackers back off and fall through tiers | `tracker_tests.rs` |
| Malformed compact peers | 6-byte (and 18-byte for `peers6`) size checks → `InvalidCompactPeers`/`InvalidCompactPeers6` | `tracker_tests.rs` |
| Bogus peer addresses (loopback, unspecified, broadcast, multicast, port 0) | `is_valid_peer_address` filters before the backlog | `tracker.rs`; exercised in engine tests |
| Endless redirect loops | Redirect handling bounded by the HTTP client; loops surface as tracker errors | `tracker_tests.rs` (`spawn_redirect_loop_http_tracker`) |

## Peer wire — remote

| Hostile input | Cap / check | Test |
| --- | --- | --- |
| Oversized / malformed frames | Hard 1 MiB `MAX_MESSAGE_LENGTH` → disconnect (`OversizedMessage`); per-message payload length validation | `peer/message.rs` tests |
| Handshake with wrong length / protocol / info hash / our own peer id | Rejected before any state is kept (`HandshakeError`) | `peer/handshake.rs` tests |
| Out-of-range block requests from peers (upload path) | 16 KiB `MAX_REQUEST_LENGTH`; 32 KiB `MAX_TOLERATED_LENGTH` → disconnect; index/begin/length bounds via `Storage` (`PieceOutOfRange`) | `engine/peer_task.rs` tests |
| Unexpected, duplicate, misaligned or wrong-sized blocks | `PieceAssembler` rejects them (`Unexpected`/`Duplicate`); only full, aligned blocks are accepted | `assembly.rs` tests |
| Bitfields with spare bits / wrong length | `BitfieldError::SpareBitsSet`/`InvalidLength` | `peer/bitfield.rs` tests |
| Corrupt pieces (bad hash) | Contributor strikes; ban after 3 strikes; the piece is re-requested | `engine` tests (`bans_peer_after_three_strikes`); `seeding_e2e` `CorruptOnce` |
| Choke/interest spam | Flags only; chokes return in-flight blocks; no unbounded state grows | `engine` tests |

## Metadata exchange (ut_metadata, BEP 9) — remote

| Hostile input | Cap / check | Test |
| --- | --- | --- |
| Metadata larger than 10 MiB | `MAX_METADATA_SIZE`; the advertising peer is marked untrusted | `seeding_e2e` `OversizedHandshake` |
| Zero or conflicting `metadata_size` | Peer marked untrusted; conflicting sizes rejected | `seeding_e2e` `ZeroHandshake`, `ConflictingDataSize` |
| Out-of-range or wrong-sized metadata pieces | Piece index bounds and per-piece size checks (`metadata_piece_len`) | `seeding_e2e` `UnsolicitedExtras` |
| Assembled metadata that does not hash to the info hash | Assembly rejected; repeated failures (8) ban all contributors | `seeding_e2e` `Corrupt`; `MAX_METADATA_ASSEMBLY_FAILURES` in `engine/mod.rs` |
| Unsolicited / duplicated metadata messages | Ignored unless requested; requesters are deduplicated | `seeding_e2e` `UnsolicitedExtras` |

## DHT (BEP 5) — remote UDP

| Hostile input | Cap / check | Test |
| --- | --- | --- |
| Oversized datagrams | `MAX_DATAGRAM_SIZE` = 2048 bytes | `dht/krpc.rs` |
| Bogon / loopback / multicast addresses | `AddressFilter::strict` before anything is stored; V6 rejected (IPv4-only DHT) | `dht/filter.rs` |
| Sybil attacks (many nodes per IP) | `MAX_NODES_PER_IP` = 2 in table and persistence | `dht/table.rs`, `dht/state.rs` |
| Node table poisoning | Nodes join only after answering a ping; two failed queries evict | `dht/table.rs` tests; `dht_e2e` |
| Forged announce tokens | Announces require a valid, single-use token issued to that address | `dht/tokens.rs` |
| Unbounded peer stores | `MAX_PEERS_PER_INFO_HASH` = 100, `MAX_INFO_HASHES` = 2048, `MAX_TOTAL_PEERS` = 8192 | `dht/store.rs` |
| Query floods | `MAX_PENDING_QUERIES` = 128, `MAX_CONCURRENT_LOOKUPS` = 4, per-IP limiter with `MAX_TRACKED_IPS` = 4096 | `dht/service.rs`, `dht/limiter.rs` |
| Oversized node/peer lists in responses | Clamped to 8 nodes / 25 peers per response | `dht/service.rs` |
| Corrupt persisted DHT state | Invalid entries dropped with a reported count; corrupt file starts fresh | `session` restore path; `dht/state.rs` |
| Private torrents | No DHT reserved bit, no lookups, no announces, no Port relay | `engine/mod.rs` (`is_private` gating), `dht` tests |

## Resume snapshots and session state — local disk

| Hostile input | Cap / check | Test |
| --- | --- | --- |
| Tampered / truncated snapshot | Size cap 4 MiB (`MAX_RESUME_FILE_BYTES`), JSON validity, format version, info hash, piece count, file count/length all verified; anything odd falls back to a full recheck | `resume.rs` tests (`corrupt_and_truncated_files_are_rejected`, `unknown_version_is_rejected`, `wrong_info_hash_is_rejected`, `wrong_piece_count_is_rejected`) |
| Fingerprint mismatch (file edited or deleted) | Only the overlapping pieces are re-verified; missing files count only where they should exist | `resume.rs` tests; `file_selection_tests.rs` |
| Corrupt session.json | Reported as a restore error; the session starts empty; new fields have serde defaults | `session/persist.rs`; session tests |

## Inbound connections — remote

| Hostile input | Cap / check | Test |
| --- | --- | --- |
| Handshakes for unknown info hashes | The listener only accepts info hashes registered by live torrents | `listener.rs` tests |
| Connection floods | `MAX_PEERS` = 50, `MAX_PEERS_PER_IP` = 8, banned set checked first | `engine/mod.rs`; engine tests |
| Storage fault spam (disk full etc.) | Typed errors, retryable flag, engine stops requesting; no panic, no spin, no peer strikes | `disk_error_tests.rs` |
| Long paths | Components validated ≤255 UTF-16 units; totals ≥260 UTF-16 units go through the verbatim `\\?\` prefix or fail with a typed error | `paths.rs` tests |

## Residual risks

- The DHT, tracker and peer protocols are spoken over unencrypted transports;
  traffic is observable and forgeable at the network level.
- The upload path serves whatever `Storage` reads for verified pieces; a
  tampered local file is served as-is (integrity is the torrent's hash, and
  `force_recheck` re-verification is the user's tool against it).
- The desktop app runs with the user's privileges; the download directory is
  taken from settings, and only path-validated, collision-checked layouts are
  ever written beneath it.
- The Windows binary and installer are unsigned.
