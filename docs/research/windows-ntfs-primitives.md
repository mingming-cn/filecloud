# Windows/NTFS filesystem primitive spike

Status: the opt-in unified 1B gate has passed on a local fixed NTFS checkout.
Future platform or filesystem changes must rerun it; cross compilation only
validates declarations and linkage, not NTFS behaviour.

## Binding boundary

A worktree is accepted only when the opened directory handle reports `NTFS`,
the volume root is `DRIVE_FIXED`, and its capabilities include reparse points,
hard links, persistent ACLs, and Unicode names. Network and mapped drives are
rejected before binding. A case-sensitive directory is rejected because the
sync protocol treats case-fold collisions as non-unique.

All later Windows child opens must be relative to an already verified parent
handle. The required implementation is `NtCreateFile` with
`OBJECT_ATTRIBUTES.RootDirectory`, `OBJ_DONT_REPARSE`, and
`FILE_OPEN_REPARSE_POINT`; a pathname-based `CreateFileW` is not an acceptable
replacement because a concurrent junction or symlink replacement could leave
the verified worktree.

## Required runtime proof

`TestWindowsNTFSPrimitives` and `TestCrossPlatformAcceptanceMatrix` record:

1. Reparse points cannot be traversed while scanning, checkout, or recovery.
2. Opened files retain a stable `(volume serial, file ID, type, link count)`
   identity, and IDs that cannot be safely persisted are refused.
3. A handle-relative no-replace rename leaves an existing destination intact.
4. A same-directory rename and parent directory flush complete on NTFS.
5. An independently started process cannot acquire a held binding lock.
6. A source held without delete sharing makes checkout rename fail explicitly;
   the source remains intact and a restart does not delete user content.

The gate is run only with:

```powershell
$env:FILECLOUD_RUN_1B=1
go test ./cmd/filecloud -run '^TestCrossPlatformAcceptanceMatrix$' -count=1 -timeout=30m -v
```

Acceptance record from 2026-08-15: the unified gate passed on a local fixed NTFS
checkout at commit `754e0bb`, including every required scenario and all 112
structured attestations. Deterministic convergence produced Root
`746abb68bf9a6fb9cc2251b96be467622a28dd8a7d6686c73a5f0db97a703599` and Head
`6b604c48ae82f9a829370976c3f08ce34dee7ea01a1791a336b3363175fcee53`.

Acceptance rerun from 2026-08-16: commit `ff9afc3` passed the same unified gate
on Windows 11 (Microsoft Windows NT 10.0.26200.0, amd64) with Go 1.26.6 and a
healthy local fixed NTFS volume presented by KVM/QEMU. Every required scenario
and all 112 structured attestations passed; deterministic Root and Head matched
the frozen values above. The first rerun exposed a non-portable test assertion
that required an immediate measurement to have a positive clock delta;
`ff9afc3` removed that timer-resolution assumption before all three platform
gates were rerun.

## Primary sources

- Microsoft, [NtCreateFile] (https://learn.microsoft.com/windows-hardware/drivers/ddi/ntifs/nf-ntifs-ntcreatefile): relative root handles and create options.
- Microsoft, [Reparse Point Operations](https://learn.microsoft.com/windows/win32/fileio/reparse-point-operations): reparse capability and operations.
- Microsoft, [FILE_ID_INFO](https://learn.microsoft.com/windows/win32/api/winbase/ns-winbase-file_id_info): volume serial and file ID identity.
- Microsoft, [FILE_RENAME_INFO](https://learn.microsoft.com/windows/win32/api/winbase/ns-winbase-file_rename_info): `ReplaceIfExists` rename semantics.
- Microsoft, [GetVolumeInformationByHandleW](https://learn.microsoft.com/windows/win32/api/fileapi/nf-fileapi-getvolumeinformationbyhandlew): filesystem and capability inspection.
- Microsoft, [LockFileEx](https://learn.microsoft.com/windows/win32/api/fileapi/nf-fileapi-lockfileex): cross-process byte-range locking.
- Microsoft, [FlushFileBuffers](https://learn.microsoft.com/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers): file and directory flush limitations.
