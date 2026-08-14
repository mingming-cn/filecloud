package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	fscompat "github.com/mingming-cn/filecloud/internal/fscompat"
	"github.com/mingming-cn/filecloud/internal/object"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

const (
	fsJournalFormat = 1

	fsPhasePreBase  = "pre_base"
	fsPhaseRollback = "rollback"
	fsPhasePostBase = "post_base"

	fsOpCreateFile       = "create_file"
	fsOpCreateDirectory  = "create_directory"
	fsOpRename           = "rename"
	fsOpRestorePromotion = "restore_promotion"
	fsOpUnlink           = "unlink"
	fsOpRmdir            = "rmdir"
	fsOpMtime            = "mtime"

	fsStateIntent    = "intent"
	fsStateCompleted = "completed"

	fsActionInternalPrefix         = ".filecloud-internal-action-"
	fsPromotionTargetParentPrefix  = "fpt1:"
	fsPromotionFallbackOwnerPrefix = "fpr1:"
)

var (
	errFSJournalRootChanged = errors.New("worktree root identity changed since journal creation")
	_openActionParent       = openActionParent
)

type fsAction struct {
	Worktree                                       string
	ActionID, OriginActionID                       string
	Order                                          int64
	Attempt                                        int
	Phase, Op, Parent                              string
	ParentDevice, ParentInode                      uint64
	Source, Target, ExpectedKind                   string
	ExpectedDevice, ExpectedInode                  uint64
	ExpectedObject, ExpectedMtime                  string
	ExpectedSize                                   int64
	InternalSource, InternalTarget, Outcome, State string
}

type fsActionFault func(point string, action fsAction) error

func newFSActionID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func validFSActionID(value string) bool {
	if len(value) != 32 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 16
}

func validFSInternalName(value string) bool {
	for _, prefix := range []string{fsActionInternalPrefix, checkoutTempPrefix, syncTombstonePrefix, syncRecoveryPrefix} {
		if strings.HasPrefix(value, prefix) && validFSActionID(strings.TrimPrefix(value, prefix)) {
			return true
		}
	}
	return false
}

func validateFSRelativeParent(value string) bool {
	if value == "" {
		return true
	}
	if !utf8.ValidString(value) || strings.HasPrefix(value, "/") || strings.Contains(value, "\\") || strings.ContainsRune(value, 0) {
		return false
	}
	for _, component := range strings.Split(value, "/") {
		if component == "" || component == "." || component == ".." ||
			(strings.HasPrefix(component, ".filecloud-internal-") && !validFSInternalName(component)) {
			return false
		}
	}
	return true
}

func validateFSLeaf(value, internalName string, optional bool) bool {
	if value == "" {
		return optional && internalName == ""
	}
	if !utf8.ValidString(value) || value == "." || value == ".." || strings.ContainsAny(value, "/\\") || strings.ContainsRune(value, 0) {
		return false
	}
	if strings.HasPrefix(value, ".filecloud-internal-") {
		return value == internalName && validFSInternalName(internalName)
	}
	return internalName == ""
}

func validRecoveryVisibleName(name string) bool {
	if name == "" || name != norm.NFC.String(name) || len(name) > 240 || name == "." || name == ".." ||
		strings.HasPrefix(name, ".filecloud-internal-") || strings.HasSuffix(name, " ") || strings.HasSuffix(name, ".") {
		return false
	}
	for _, r := range name {
		if r <= 0x1f || strings.ContainsRune(`<>:"/\\|?*`, r) {
			return false
		}
	}
	base := name
	if index := strings.IndexByte(base, '.'); index >= 0 {
		base = base[:index]
	}
	upper := strings.ToUpper(base)
	return upper != "CON" && upper != "PRN" && upper != "AUX" && upper != "NUL" &&
		!(len(upper) == 4 && (strings.HasPrefix(upper, "COM") || strings.HasPrefix(upper, "LPT")) && upper[3] >= '1' && upper[3] <= '9')
}

func recoveryVisibleLeaf(parent, actionID string, counter int) (string, error) {
	if !validateFSRelativeParent(parent) || !validFSActionID(actionID) || counter < 1 || counter > 9999 {
		return "", errors.New("invalid visible recovery candidate input")
	}
	available := 1024
	if parent != "" {
		available -= len(parent) + 1
	}
	available = min(available, 240)
	if available < 1 {
		return "", errors.New("visible recovery target exceeds path limits")
	}
	stem := "Filecloud recovered " + actionID[:12]
	if available >= 6 {
		suffix := ""
		if counter > 1 {
			suffix = " " + strconv.Itoa(counter)
		}
		candidate := truncateUTF8(stem, available-len(suffix)) + suffix
		if validRecoveryVisibleName(candidate) {
			return candidate, nil
		}
		return "", errors.New("visible recovery target is invalid")
	}

	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	capacity := uint64(1)
	for range available {
		capacity *= uint64(len(alphabet))
	}
	digest := sha256.Sum256([]byte(actionID))
	offset := uint64(0)
	for _, value := range digest[:8] {
		offset = offset<<8 | uint64(value)
	}
	seen := 0
	for step := uint64(0); step < capacity && seen < 9999; step++ {
		value := (offset + step) % capacity
		candidate := make([]byte, available)
		for index := len(candidate) - 1; index >= 0; index-- {
			candidate[index] = alphabet[value%uint64(len(alphabet))]
			value /= uint64(len(alphabet))
		}
		if !validRecoveryVisibleName(string(candidate)) {
			continue
		}
		seen++
		if seen == counter {
			return string(candidate), nil
		}
	}
	return "", errors.New("visible recovery target collision sequence exhausted")
}

func truncateUTF8(value string, limit int) string {
	if limit <= 0 {
		return ""
	}
	if len(value) <= limit {
		return value
	}
	for limit > 0 && !utf8.RuneStart(value[limit]) {
		limit--
	}
	return value[:limit]
}

func encodePromotionTargetParent(device, inode uint64) string {
	return fmt.Sprintf("%s%016x:%016x", fsPromotionTargetParentPrefix, device, inode)
}

func decodePromotionTargetParent(value string) (uint64, uint64, error) {
	if len(value) != len(fsPromotionTargetParentPrefix)+16+1+16 ||
		!strings.HasPrefix(value, fsPromotionTargetParentPrefix) || value[len(fsPromotionTargetParentPrefix)+16] != ':' {
		return 0, 0, errors.New("filesystem promotion target parent identity has invalid encoding")
	}
	device, deviceErr := strconv.ParseUint(value[len(fsPromotionTargetParentPrefix):len(fsPromotionTargetParentPrefix)+16], 16, 64)
	inode, inodeErr := strconv.ParseUint(value[len(fsPromotionTargetParentPrefix)+17:], 16, 64)
	if deviceErr != nil || inodeErr != nil || device == 0 || inode == 0 || encodePromotionTargetParent(device, inode) != value {
		return 0, 0, errors.New("filesystem promotion target parent identity has invalid encoding")
	}
	return device, inode, nil
}

func fallbackRootCreateOwner(value fsAction) (string, bool) {
	if value.Op != fsOpCreateDirectory || value.InternalSource != "" ||
		!strings.HasPrefix(value.InternalTarget, fsPromotionFallbackOwnerPrefix) {
		return "", false
	}
	owner := strings.TrimPrefix(value.InternalTarget, fsPromotionFallbackOwnerPrefix)
	return owner, validFSActionID(owner)
}

func validateFSAction(value fsAction) error {
	linked := value.OriginActionID != ""
	promotion := value.Op == fsOpRename && value.ExpectedObject != ""
	restorePromotion := value.Op == fsOpRestorePromotion
	_, fallbackRootCreate := fallbackRootCreateOwner(value)
	preserve := linked && !promotion
	if value.Worktree == "" || !validFSActionID(value.ActionID) || value.Order < 0 ||
		(value.Phase != fsPhasePreBase && value.Phase != fsPhaseRollback && value.Phase != fsPhasePostBase) ||
		(value.State != fsStateIntent && value.State != fsStateCompleted) || !validateFSRelativeParent(value.Parent) ||
		value.ParentDevice == 0 || value.ParentInode == 0 ||
		((value.Op != fsOpCreateFile && value.Op != fsOpCreateDirectory && !preserve) &&
			(value.ExpectedDevice == 0 || value.ExpectedInode == 0)) {
		return errors.New("filesystem action journal contains invalid fixed fields")
	}
	if linked != (value.Attempt >= 1) || (!linked && value.Attempt != 0) ||
		(linked && (!validFSActionID(value.OriginActionID) || value.OriginActionID == value.ActionID)) {
		return errors.New("filesystem action journal contains invalid action provenance")
	}
	if (value.ExpectedDevice == 0) != (value.ExpectedInode == 0) {
		return errors.New("filesystem action journal contains partial expected identity")
	}
	if preserve {
		if value.Op != fsOpRename || value.Phase != fsPhaseRollback || value.ExpectedDevice != 0 || value.ExpectedInode != 0 ||
			value.InternalSource != value.Source || value.InternalTarget != "" ||
			(value.State == fsStateIntent && value.Outcome != "preserve_unknown") ||
			(value.State == fsStateCompleted && value.Outcome != "preserve_unknown" && value.Outcome != "collision") {
			return errors.New("filesystem action journal contains invalid preserve outcome")
		}
	} else if linked && (!promotion || value.Phase != fsPhasePreBase || value.Outcome != "" || value.InternalSource != "") {
		return errors.New("filesystem action journal contains invalid promotion linkage")
	} else if value.Outcome != "" && !(value.State == fsStateCompleted &&
		((value.Op == fsOpCreateFile || value.Op == fsOpCreateDirectory) && value.Outcome == "rolled_back" ||
			fallbackRootCreate && value.Outcome == "collision")) {
		return errors.New("filesystem action journal contains invalid outcome")
	}
	if !preserve && value.State == fsStateCompleted && (value.Op == fsOpCreateFile || value.Op == fsOpCreateDirectory) &&
		((value.Outcome == "" && value.ExpectedDevice == 0) || (value.Outcome == "rolled_back" && value.ExpectedDevice != 0) ||
			(fallbackRootCreate && value.Outcome == "collision" && (value.ExpectedDevice != 0 || value.ExpectedInode != 0))) {
		return errors.New("filesystem completed creation has invalid identity or outcome")
	}
	if (value.InternalSource != "" && !validFSInternalName(value.InternalSource)) ||
		(value.InternalTarget != "" && !validFSInternalName(value.InternalTarget) && !promotion && !fallbackRootCreate) {
		return errors.New("filesystem action journal contains invalid internal ownership")
	}
	if fallbackRootCreate {
		if value.Parent != "" || !validFallbackRootName(value.Source) || value.Target != "" ||
			value.ExpectedKind != "Directory" || value.ExpectedObject != "" || value.ExpectedSize != 0 ||
			value.ExpectedMtime != "" || value.OriginActionID != "" || value.Attempt != 0 {
			return errors.New("filesystem fallback root creation action is invalid")
		}
	} else if !validateFSLeaf(value.Source, value.InternalSource, value.Op == fsOpMtime) {
		return errors.New("filesystem action journal contains invalid source leaf")
	}
	if promotion {
		targetParent, targetLeaf := splitFSActionPath(value.Target)
		if !validateFSRelativeParent(targetParent) || !validRecoveryVisibleName(targetLeaf) {
			return errors.New("filesystem promotion action contains invalid target path")
		}
		if value.InternalTarget != "" {
			if _, _, err := decodePromotionTargetParent(value.InternalTarget); err != nil {
				return err
			}
		}
	} else if restorePromotion {
		components := strings.Split(value.Target, "/")
		if !validateFSRelativeParent(value.Target) || len(components) == 0 ||
			!strings.HasPrefix(components[0], syncRecoveryPrefix) ||
			!validFSActionID(strings.TrimPrefix(components[0], syncRecoveryPrefix)) || value.InternalTarget != "" {
			return errors.New("filesystem restore promotion action contains invalid hidden target path")
		}
	} else if fallbackRootCreate {
		// The tagged internal_target durably links this visible directory creation to its promotion root.
	} else if !validateFSLeaf(value.Target, value.InternalTarget, value.Op != fsOpRename) {
		return errors.New("filesystem action journal contains invalid target leaf")
	}
	if value.ExpectedKind != "File" && value.ExpectedKind != "Directory" {
		return errors.New("filesystem action journal contains invalid expected kind")
	}
	if value.ExpectedObject != "" && !object.ValidID(value.ExpectedObject) {
		return errors.New("filesystem action journal contains invalid expected object")
	}
	if value.ExpectedSize < 0 {
		return errors.New("filesystem action journal contains invalid expected size")
	}
	if value.ExpectedMtime != "" {
		if value.Op == fsOpMtime && value.Phase == fsPhaseRollback {
			if parsed, err := time.Parse(time.RFC3339Nano, value.ExpectedMtime); err != nil || parsed.UTC().Format(time.RFC3339Nano) != value.ExpectedMtime {
				return errors.New("filesystem action journal contains invalid raw rollback root mtime")
			}
		} else if _, err := parseCanonicalProtocolMtime(value.ExpectedMtime); err != nil {
			return errors.New("filesystem action journal contains invalid expected mtime")
		}
	}
	switch value.Op {
	case fsOpCreateFile:
		if value.ExpectedKind != "File" || value.Source == "" || value.Target != "" || value.Source != value.InternalSource {
			return errors.New("filesystem file creation action is invalid")
		}
	case fsOpCreateDirectory:
		if value.ExpectedKind != "Directory" || value.Source == "" || value.Target != "" ||
			(!fallbackRootCreate && value.Source != value.InternalSource) {
			return errors.New("filesystem directory creation action is invalid")
		}
	case fsOpRename:
		if value.Source == "" || value.Target == "" || value.Source == value.Target {
			return errors.New("filesystem rename action is invalid")
		}
		if promotion && (value.ExpectedKind != "File" || value.ExpectedMtime == "" ||
			(value.InternalSource != "" && value.InternalSource != value.Source)) {
			return errors.New("filesystem promotion action is invalid")
		}
	case fsOpRestorePromotion:
		if linked || value.Phase != fsPhaseRollback || value.Source == "" || value.Target == "" ||
			value.ExpectedKind != "File" || value.ExpectedDevice == 0 || value.ExpectedInode == 0 ||
			value.ExpectedObject != "" || value.ExpectedSize != 0 ||
			value.InternalSource != "" || value.InternalTarget != "" || value.Outcome != "" {
			return errors.New("filesystem restore promotion action is invalid")
		}
	case fsOpUnlink:
		if value.ExpectedKind != "File" || value.Source == "" || value.Target != "" ||
			value.ExpectedObject == "" || value.ExpectedMtime == "" {
			return errors.New("filesystem unlink action is invalid")
		}
	case fsOpRmdir:
		if value.ExpectedKind != "Directory" || value.Source == "" || value.Target != "" {
			return errors.New("filesystem rmdir action is invalid")
		}
	case fsOpMtime:
		if (value.Source == "" && value.Parent != "") || value.Target != "" || value.ExpectedMtime == "" {
			return errors.New("filesystem mtime action is invalid")
		}
	default:
		return errors.New("filesystem action journal contains invalid operation")
	}
	return nil
}

func bindFSJournalRoot(ctx context.Context, db *sql.DB, worktree string, root *openedWorktree) error {
	if root == nil || worktree == "" || root.path != worktree {
		return errors.New("invalid worktree root journal binding")
	}
	if err := root.validateIdentity(); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, `INSERT INTO fs_journal_bindings(worktree, root_device, root_inode, journal_format)
		VALUES (?, ?, ?, ?) ON CONFLICT(worktree) DO NOTHING`, worktree, root.device, root.inode, fsJournalFormat)
	if err != nil {
		return fmt.Errorf("bind filesystem journal root: %w", err)
	}
	var device, inode uint64
	var format int
	if err := db.QueryRowContext(ctx, `SELECT root_device, root_inode, journal_format FROM fs_journal_bindings WHERE worktree = ?`, worktree).Scan(&device, &inode, &format); err != nil {
		return fmt.Errorf("read filesystem journal root: %w", err)
	}
	if device != root.device || inode != root.inode || format != fsJournalFormat {
		return errFSJournalRootChanged
	}
	return nil
}

func openFSActionParent(root *openedWorktree, relative string, expectedDevice, expectedInode uint64) (*os.File, error) {
	if root == nil || !validateFSRelativeParent(relative) {
		return nil, errors.New("invalid filesystem action parent")
	}
	if err := root.validateIdentity(); err != nil {
		return nil, err
	}
	var fd int
	var err error
	if relative == "" {
		fd, err = fscompat.Dup(int(root.directory.Fd()))
	} else {
		fd, err = _openActionParent(root, relative)
	}
	if err != nil {
		return nil, fmt.Errorf("open filesystem action parent %q: %w", relative, err)
	}
	parent := os.NewFile(uintptr(fd), relative)
	var stat fscompat.Stat_t
	if err := fscompat.Fstat(fd, &stat); err != nil {
		return nil, errors.Join(err, parent.Close())
	}
	if stat.Mode&fscompat.S_IFMT != fscompat.S_IFDIR || uint64(stat.Dev) != root.device ||
		(expectedDevice != 0 && (uint64(stat.Dev) != expectedDevice || stat.Ino != expectedInode)) {
		return nil, errors.Join(errors.New("filesystem action parent identity or mount changed"), parent.Close())
	}
	return parent, nil
}

func openFSActionParentFallback(root *openedWorktree, relative string) (int, error) {
	current, err := fscompat.Dup(int(root.directory.Fd()))
	if err != nil {
		return -1, err
	}
	for _, component := range strings.Split(relative, "/") {
		next, openErr := fscompat.Openat(current, component, fscompat.O_RDONLY|fscompat.O_DIRECTORY|fscompat.O_NOFOLLOW|fscompat.O_CLOEXEC, 0)
		fscompat.Close(current)
		if openErr != nil {
			return -1, openErr
		}
		var stat fscompat.Stat_t
		if err := fscompat.Fstat(next, &stat); err != nil || uint64(stat.Dev) != root.device {
			fscompat.Close(next)
			return -1, errors.Join(errors.New("filesystem action crossed a mount"), err)
		}
		current = next
	}
	return current, nil
}

type fsActionExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func insertFSActionIntentWith(ctx context.Context, db fsActionExecer, value fsAction) error {
	value.State = fsStateIntent
	if err := validateFSAction(value); err != nil {
		return err
	}
	var origin any
	if value.OriginActionID != "" {
		origin = value.OriginActionID
	}
	_, err := db.ExecContext(ctx, `INSERT INTO fs_actions(worktree, action_id, origin_action_id, attempt, action_order, phase, op, parent_path,
		parent_device, parent_inode, source_name, target_name, expected_kind, expected_device, expected_inode,
		expected_object, expected_size, expected_mtime, internal_source, internal_target, action_outcome, state)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, value.Worktree, value.ActionID,
		origin, value.Attempt, value.Order, value.Phase, value.Op, value.Parent, value.ParentDevice,
		value.ParentInode, value.Source, value.Target, value.ExpectedKind, value.ExpectedDevice, value.ExpectedInode,
		value.ExpectedObject, value.ExpectedSize, value.ExpectedMtime, value.InternalSource, value.InternalTarget,
		value.Outcome, value.State)
	if err != nil {
		return fmt.Errorf("persist filesystem action Intent: %w", err)
	}
	return nil
}

func insertFSActionIntent(ctx context.Context, db *sql.DB, value fsAction) error {
	return insertFSActionIntentWith(ctx, db, value)
}

func executeFSAction(ctx context.Context, db *sql.DB, root *openedWorktree, value fsAction, fault fsActionFault) error {
	if err := validatePendingCheckoutState(ctx, db, value.Worktree); err != nil {
		return err
	}
	if err := bindFSJournalRoot(ctx, db, value.Worktree, root); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := insertFSActionIntentWith(ctx, tx, value); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if fault != nil {
		if err := fault("before_intent_commit", value); err != nil {
			return errors.Join(err, tx.Rollback())
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit filesystem action Intent: %w", err)
	}
	if fault != nil {
		if err := fault("after_intent_commit", value); err != nil {
			return err
		}
	}
	return completeFSAction(ctx, db, root, value, fault)
}

func actionPathState(parent *os.File, name string, value fsAction) (bool, bool, error) {
	var stat fscompat.Stat_t
	if name == "" {
		if value.Op != fsOpMtime {
			return false, false, nil
		}
		err := fscompat.Fstat(int(parent.Fd()), &stat)
		matches := err == nil && value.ExpectedKind == "Directory" && stat.Mode&fscompat.S_IFMT == fscompat.S_IFDIR &&
			uint64(stat.Dev) == value.ExpectedDevice && stat.Ino == value.ExpectedInode
		return err == nil, matches, err
	}
	err := fscompat.Fstatat(int(parent.Fd()), name, &stat, fscompat.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, fscompat.ENOENT) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	if value.Outcome == "preserve_unknown" {
		return true, true, nil
	}
	mode := uint32(fscompat.S_IFREG)
	if value.ExpectedKind == "Directory" {
		mode = fscompat.S_IFDIR
	}
	matches := uint32(stat.Mode)&fscompat.S_IFMT == mode
	if value.ExpectedDevice != 0 || value.ExpectedInode != 0 {
		matches = matches && uint64(stat.Dev) == value.ExpectedDevice && stat.Ino == value.ExpectedInode
	}
	if value.ExpectedKind == "File" && stat.Nlink != 1 {
		matches = false
	}
	return true, matches, nil
}

func verifyFSRemovalObject(parent *os.File, value fsAction) error {
	if value.Op != fsOpUnlink || value.ExpectedObject == "" {
		return nil
	}
	file, info, err := openScannableAt(parent, value.Source, value.Parent+"/"+value.Source)
	if err != nil {
		return err
	}
	defer file.Close()
	mtime := canonicalProtocolMtime(info.ModTime())
	snapshot := worktreeSnapshot{blocks: make(map[string]blockSource)}
	id, err := scanRegularFile(file, value.Source, info, &snapshot)
	if err != nil || id != value.ExpectedObject || info.Size() != value.ExpectedSize || mtime != value.ExpectedMtime {
		return errors.Join(errors.New("filesystem removal action source content changed"), err)
	}
	return nil
}

func rollbackZeroIdentityCreate(ctx context.Context, db *sql.DB, root *openedWorktree, value fsAction,
	_ fscompat.Stat_t, fault fsActionFault) error {
	target, err := recoveryVisibleLeaf(value.Parent, value.ActionID, 1)
	if err != nil {
		return err
	}
	id, err := newFSActionID()
	if err != nil {
		return err
	}
	var rootStat fscompat.Stat_t
	if err := fscompat.Fstat(int(root.directory.Fd()), &rootStat); err != nil {
		return err
	}
	rootMtimeNS := filesystemMtimeNS(time.Unix(rootStat.Mtim.Sec, rootStat.Mtim.Nsec))
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	fail := func(err error) error { return errors.Join(err, tx.Rollback()) }
	var order int64
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(action_order), -1) + 1 FROM fs_actions WHERE worktree = ?", value.Worktree).Scan(&order); err != nil {
		return fail(err)
	}
	rollback := fsAction{Worktree: value.Worktree, ActionID: id, OriginActionID: value.ActionID, Attempt: 1,
		Order: order, Phase: fsPhaseRollback, Op: fsOpRename, Parent: value.Parent,
		ParentDevice: value.ParentDevice, ParentInode: value.ParentInode,
		Source: value.Source, Target: target, ExpectedKind: value.ExpectedKind,
		InternalSource: value.Source, Outcome: "preserve_unknown", State: fsStateIntent}
	if err := insertFSActionIntentWith(ctx, tx, rollback); err != nil {
		return fail(err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE fs_actions SET state = 'completed', action_outcome = 'rolled_back'
		WHERE worktree = ? AND action_id = ? AND state = 'intent' AND expected_device = 0 AND expected_inode = 0`,
		value.Worktree, value.ActionID)
	if err != nil {
		return fail(err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return fail(errors.Join(errors.New("mark ambiguous filesystem creation rolled back"), err))
	}
	if _, err := tx.ExecContext(ctx, `UPDATE pending_checkouts SET apply_state = 'rolling_back',
		rollback_root_mtime_ns = CASE WHEN rollback_root_mtime_valid=0 THEN ? ELSE rollback_root_mtime_ns END,
		rollback_root_mtime_valid = 1 WHERE worktree = ? AND apply_state IN ('pending', 'applying')`,
		rootMtimeNS, value.Worktree); err != nil {
		return fail(err)
	}
	if fault != nil {
		if err := fault("before_intent_commit", rollback); err != nil {
			return fail(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if fault != nil {
		if err := fault("after_intent_commit", rollback); err != nil {
			return err
		}
	}
	if err := completeFSAction(ctx, db, root, rollback, fault); err != nil {
		return fmt.Errorf("preserve ambiguous filesystem creation as %q: %w", target, err)
	}
	return fmt.Errorf("filesystem creation identity was not durable; preserved unknown path as %q", target)
}

func advancePreserveUnknownCollision(ctx context.Context, db *sql.DB, root *openedWorktree,
	value fsAction, fault fsActionFault) error {
	target, err := recoveryVisibleLeaf(value.Parent, value.OriginActionID, value.Attempt+1)
	if err != nil {
		return err
	}
	id, err := newFSActionID()
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	fail := func(err error) error { return errors.Join(err, tx.Rollback()) }
	var order int64
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(action_order), -1) + 1 FROM fs_actions WHERE worktree = ?", value.Worktree).Scan(&order); err != nil {
		return fail(err)
	}
	successor := value
	successor.ActionID, successor.Attempt, successor.Order, successor.Target = id, value.Attempt+1, order, target
	successor.State, successor.Outcome = fsStateIntent, "preserve_unknown"
	if err := insertFSActionIntentWith(ctx, tx, successor); err != nil {
		return fail(err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE fs_actions SET state='completed', action_outcome='collision'
		WHERE worktree=? AND action_id=? AND state='intent' AND action_outcome='preserve_unknown'`, value.Worktree, value.ActionID)
	if err != nil {
		return fail(err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return fail(errors.Join(errors.New("advance visible recovery collision action"), err))
	}
	journal, err := loadFSActionsWith(ctx, tx, value.Worktree)
	if err != nil {
		return fail(err)
	}
	if err := validateFSActionJournal(ctx, tx, value.Worktree, journal); err != nil {
		return fail(err)
	}
	if fault != nil {
		if err := fault("before_intent_commit", successor); err != nil {
			return fail(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if fault != nil {
		if err := fault("after_intent_commit", successor); err != nil {
			return err
		}
	}
	return completeFSAction(ctx, db, root, successor, fault)
}

func completeFallbackRootCollision(ctx context.Context, db *sql.DB, value fsAction, fault fsActionFault) error {
	result, err := db.ExecContext(ctx, `UPDATE fs_actions SET state='completed',action_outcome='collision'
		WHERE worktree=? AND action_id=? AND state='intent' AND expected_device=0 AND expected_inode=0`,
		value.Worktree, value.ActionID)
	if err != nil {
		return err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return errors.Join(errors.New("complete fallback root collision"), err)
	}
	value.State, value.Outcome = fsStateCompleted, "collision"
	if fault != nil {
		return fault("after_completed", value)
	}
	return nil
}

func completeFSAction(ctx context.Context, db *sql.DB, root *openedWorktree, value fsAction, fault fsActionFault) error {
	if err := validatePendingCheckoutState(ctx, db, value.Worktree); err != nil {
		return err
	}
	if err := validateFSAction(value); err != nil {
		return err
	}
	journal, err := loadFSActions(ctx, db, value.Worktree)
	if err != nil {
		return err
	}
	if err := validateFSActionJournal(ctx, db, value.Worktree, journal); err != nil {
		return err
	}
	if value.Op == fsOpRename && value.ExpectedObject != "" {
		if err := validatePromotionOwnership(ctx, db, value.Worktree, journal); err != nil {
			return err
		}
		return completePromotionAction(ctx, db, root, value, fault)
	}
	if value.Op == fsOpRestorePromotion {
		if err := validatePromotionOwnership(ctx, db, value.Worktree, journal); err != nil {
			return err
		}
		return completeRestorePromotionAction(ctx, db, root, value, fault)
	}
	parent, err := openFSActionParent(root, value.Parent, value.ParentDevice, value.ParentInode)
	if err != nil {
		return err
	}
	defer parent.Close()
	sourceExists, sourceMatches, err := actionPathState(parent, value.Source, value)
	if err != nil {
		return err
	}
	targetExists, targetMatches, err := actionPathState(parent, value.Target, value)
	if err != nil {
		return err
	}
	changed := false
	switch value.Op {
	case fsOpCreateFile, fsOpCreateDirectory:
		_, fallbackRootCreate := fallbackRootCreateOwner(value)
		if fallbackRootCreate && sourceExists && !sourceMatches {
			return completeFallbackRootCollision(ctx, db, value, fault)
		}
		if sourceExists {
			if value.ExpectedDevice == 0 || value.ExpectedInode == 0 {
				var stat fscompat.Stat_t
				if err := fscompat.Fstatat(int(parent.Fd()), value.Source, &stat, fscompat.AT_SYMLINK_NOFOLLOW); err != nil {
					return err
				}
				if _, fallbackRootCreate := fallbackRootCreateOwner(value); !fallbackRootCreate {
					return rollbackZeroIdentityCreate(ctx, db, root, value, stat, fault)
				}
				if stat.Mode&fscompat.S_IFMT != fscompat.S_IFDIR || uint64(stat.Dev) != root.device {
					return errors.New("filesystem fallback root creation collided with a non-directory")
				}
			}
			if !sourceMatches {
				return errors.New("registered filesystem creation path has unexpected type or identity")
			}
		} else {
			if fault != nil {
				if err := fault("before_action", value); err != nil {
					return err
				}
			}
			if fallbackRootCreate {
				alias, err := parentHasCasefoldAlias(parent, value.Source)
				if err != nil {
					return err
				}
				if alias {
					return completeFallbackRootCollision(ctx, db, value, fault)
				}
				var raced fscompat.Stat_t
				if err := fscompat.Fstatat(int(parent.Fd()), value.Source, &raced, fscompat.AT_SYMLINK_NOFOLLOW); err == nil {
					return completeFSAction(ctx, db, root, value, fault)
				} else if !errors.Is(err, fscompat.ENOENT) {
					return err
				}
			}
			if value.Op == fsOpCreateDirectory {
				err = fscompat.Mkdirat(int(parent.Fd()), value.Source, 0o700)
			} else {
				var fd int
				fd, err = fscompat.Openat(int(parent.Fd()), value.Source, fscompat.O_WRONLY|fscompat.O_CREAT|fscompat.O_EXCL|fscompat.O_NOFOLLOW|fscompat.O_CLOEXEC, 0o600)
				if err == nil {
					err = fscompat.Close(fd)
				}
			}
			if err != nil {
				return fmt.Errorf("execute journaled creation: %w", err)
			}
			changed = true
			if fault != nil {
				if err := fault("between_create_identity", value); err != nil {
					return err
				}
			}
		}
		var createdStat fscompat.Stat_t
		if err := fscompat.Fstatat(int(parent.Fd()), value.Source, &createdStat, fscompat.AT_SYMLINK_NOFOLLOW); err != nil {
			return err
		}
		if value.ExpectedDevice == 0 && value.ExpectedInode == 0 {
			result, err := db.ExecContext(ctx, `UPDATE fs_actions SET expected_device = ?, expected_inode = ?
				WHERE worktree = ? AND action_id = ? AND state = 'intent' AND expected_device = 0 AND expected_inode = 0`,
				uint64(createdStat.Dev), createdStat.Ino, value.Worktree, value.ActionID)
			if err != nil {
				return err
			}
			if rows, err := result.RowsAffected(); err != nil || rows != 1 {
				var state, outcome string
				var device, inode uint64
				readErr := db.QueryRowContext(ctx, `SELECT state,action_outcome,expected_device,expected_inode FROM fs_actions
					WHERE worktree=? AND action_id=?`, value.Worktree, value.ActionID).Scan(&state, &outcome, &device, &inode)
				if err == nil && readErr == nil && state == fsStateCompleted && outcome == "" &&
					device == uint64(createdStat.Dev) && inode == createdStat.Ino {
					return nil
				}
				return errors.Join(errors.New("record filesystem creation identity failed"), err, readErr)
			}
			value.ExpectedDevice, value.ExpectedInode = uint64(createdStat.Dev), createdStat.Ino
		}
	case fsOpRename:
		switch {
		case sourceExists && sourceMatches && targetExists && value.Outcome == "preserve_unknown":
			return advancePreserveUnknownCollision(ctx, db, root, value, fault)
		case sourceExists && sourceMatches && !targetExists:
			if fault != nil {
				if err := fault("before_action", value); err != nil {
					return err
				}
			}
			if err := renameNoReplace(int(parent.Fd()), value.Source, int(parent.Fd()), value.Target); err != nil {
				return fmt.Errorf("execute journaled rename: %w", err)
			}
			changed = true
		case !sourceExists && targetExists && targetMatches:
			// The atomic rename happened before the prior process stopped.
		default:
			return errors.New("filesystem rename action has ambiguous or mismatched source/target state")
		}
	case fsOpUnlink, fsOpRmdir:
		if targetExists {
			return errors.New("filesystem removal action has unexpected target")
		}
		if sourceExists {
			if !sourceMatches {
				return errors.New("filesystem removal action source identity changed")
			}
			if err := verifyFSRemovalObject(parent, value); err != nil {
				return err
			}
			if fault != nil {
				if err := fault("before_action", value); err != nil {
					return err
				}
			}
			flags := 0
			if value.Op == fsOpRmdir {
				flags = fscompat.AT_REMOVEDIR
			}
			if err := fscompat.Unlinkat(int(parent.Fd()), value.Source, flags); err != nil {
				if value.Op == fsOpRmdir && (errors.Is(err, fscompat.ENOTEMPTY) || errors.Is(err, fscompat.EEXIST)) {
					return errors.New("checkout rollback directory contains unexpected user content")
				}
				return fmt.Errorf("execute journaled removal: %w", err)
			}
			changed = true
		}
	case fsOpMtime:
		if !sourceExists || !sourceMatches {
			return errors.New("filesystem mtime action source identity changed")
		}
		if fault != nil {
			if err := fault("before_action", value); err != nil {
				return err
			}
		}
		flags := fscompat.O_RDONLY | fscompat.O_NOFOLLOW | fscompat.O_CLOEXEC
		if value.ExpectedKind == "Directory" {
			flags |= fscompat.O_DIRECTORY
		}
		var fd int
		if value.Source == "" {
			fd, err = fscompat.Dup(int(parent.Fd()))
		} else {
			fd, err = fscompat.Openat(int(parent.Fd()), value.Source, flags, 0)
		}
		if err != nil {
			return err
		}
		file := os.NewFile(uintptr(fd), value.Source)
		err = setOpenFileMtime(file, value.ExpectedMtime)
		if err == nil {
			err = file.Sync()
		}
		if closeErr := file.Close(); err != nil || closeErr != nil {
			return errors.Join(err, closeErr)
		}
		changed = true
	}
	if fault != nil && changed {
		if err := fault("after_action", value); err != nil {
			return err
		}
	}
	if err := parent.Sync(); err != nil {
		return fmt.Errorf("sync filesystem action parent: %w", err)
	}
	if fault != nil {
		if err := fault("after_parent_sync", value); err != nil {
			return err
		}
	}
	result, err := db.ExecContext(ctx, `UPDATE fs_actions SET state = 'completed'
		WHERE worktree = ? AND action_id = ? AND state = 'intent'`, value.Worktree, value.ActionID)
	if err != nil {
		return fmt.Errorf("persist filesystem action Completed: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return errors.Join(errors.New("filesystem action completion did not update one Intent"), err)
	}
	if fault != nil {
		if err := fault("after_completed", value); err != nil {
			return err
		}
	}
	if value.Outcome == "preserve_unknown" {
		return fmt.Errorf("filesystem creation identity was not durable; preserved unknown path as %q", value.Target)
	}
	return nil
}

type fsActionQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func loadFSActionsWith(ctx context.Context, db fsActionQueryer, worktree string) ([]fsAction, error) {
	rows, err := db.QueryContext(ctx, `SELECT worktree, action_id, COALESCE(origin_action_id, ''), attempt, action_order, phase, op, parent_path,
		parent_device, parent_inode, source_name, target_name, expected_kind, expected_device, expected_inode,
		expected_object, expected_size, expected_mtime, internal_source, internal_target, action_outcome, state FROM fs_actions
		WHERE worktree = ? ORDER BY action_order, action_id LIMIT ?`, worktree, maxSyncJournalRows+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []fsAction
	for rows.Next() {
		var value fsAction
		if err := rows.Scan(&value.Worktree, &value.ActionID, &value.OriginActionID, &value.Attempt, &value.Order,
			&value.Phase, &value.Op, &value.Parent,
			&value.ParentDevice, &value.ParentInode, &value.Source, &value.Target, &value.ExpectedKind,
			&value.ExpectedDevice, &value.ExpectedInode, &value.ExpectedObject, &value.ExpectedSize,
			&value.ExpectedMtime, &value.InternalSource, &value.InternalTarget, &value.Outcome, &value.State); err != nil {
			return nil, err
		}
		if err := validateFSAction(value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(values) > maxSyncJournalRows {
		return nil, errors.New("filesystem action journal exceeds synchronization budget")
	}
	return values, nil
}

func loadFSActions(ctx context.Context, db *sql.DB, worktree string) ([]fsAction, error) {
	return loadFSActionsWith(ctx, db, worktree)
}

func loadIntentFSActions(ctx context.Context, db *sql.DB, worktree string) ([]fsAction, error) {
	values, err := loadFSActions(ctx, db, worktree)
	if err != nil {
		return nil, err
	}
	intents := values[:0]
	for _, value := range values {
		if value.State == fsStateIntent {
			intents = append(intents, value)
		}
	}
	return intents, nil
}

func validateFSActionJournal(ctx context.Context, db fsActionQueryer, worktree string, values []fsAction) error {
	byID := make(map[string]fsAction, len(values))
	chains := make(map[string]map[int]fsAction)
	for _, value := range values {
		if _, exists := byID[value.ActionID]; exists {
			return errors.New("filesystem action journal contains duplicate action identity")
		}
		byID[value.ActionID] = value
		if value.OriginActionID != "" {
			if chains[value.OriginActionID] == nil {
				chains[value.OriginActionID] = make(map[int]fsAction)
			}
			if _, exists := chains[value.OriginActionID][value.Attempt]; exists {
				return errors.New("filesystem preserve chain contains duplicate attempt")
			}
			chains[value.OriginActionID][value.Attempt] = value
		}
	}
	fallbackCreates := make(map[string][]fsAction)
	for _, value := range values {
		ownerID, fallbackRootCreate := fallbackRootCreateOwner(value)
		if !fallbackRootCreate {
			continue
		}
		owner, ok := byID[ownerID]
		if !ok || owner.Worktree != worktree || owner.OriginActionID != "" || owner.Op != fsOpRename ||
			owner.ExpectedObject == "" || value.Worktree != worktree || value.Order <= owner.Order {
			return errors.New("filesystem fallback root creation has no exact promotion root owner")
		}
		fallbackCreates[ownerID] = append(fallbackCreates[ownerID], value)
	}
	for _, creates := range fallbackCreates {
		sort.Slice(creates, func(i, j int) bool { return creates[i].Order < creates[j].Order })
		previousOrdinal := 0
		for index, value := range creates {
			ordinal, ok := fallbackRootOrdinal(value.Source)
			if !ok || ordinal <= previousOrdinal || index != 0 && creates[index-1].Outcome != "collision" {
				return errors.New("filesystem fallback root creation chain is not strict or contiguous")
			}
			previousOrdinal = ordinal
		}
	}
	for originID, chain := range chains {
		origin, ok := byID[originID]
		if !ok || origin.Worktree != worktree || origin.OriginActionID != "" || origin.Attempt != 0 {
			return errors.New("filesystem action chain has an invalid root")
		}
		if origin.Op == fsOpRename && origin.ExpectedObject != "" {
			seed, sourcePath, err := promotionChainNamingSeed(ctx, db, worktree, originID, values)
			if err != nil {
				return err
			}
			previous := origin.Target
			previousAction := origin
			for attempt := 1; attempt <= len(chain); attempt++ {
				value, ok := chain[attempt]
				parent, previousLeaf := splitFSActionPath(previous)
				want, nextErr := nextPromotionChainPath(previous, seed, sourcePath)
				if nextErr == nil {
					wantParent, _ := splitFSActionPath(want)
					valueParent, _ := splitFSActionPath(value.Target)
					if wantParent == _fallbackConflictRoot && valueParent != wantParent && validFallbackRootName(valueParent) {
						_, originalLeaf := splitFSActionPath(sourcePath)
						wantLeaf, fallbackErr := _fallbackConflictName(originalLeaf, valueParent, seed, 1)
						if fallbackErr != nil {
							nextErr = fallbackErr
						} else {
							want = valueParent + "/" + wantLeaf
						}
					}
				}
				validSource := value.Source == previousLeaf || cases.Fold().String(value.Source) == cases.Fold().String(previousLeaf)
				previousTargetDevice, previousTargetInode, identityErr := decodePromotionTargetParent(previousAction.InternalTarget)
				legacyIdentity := previousAction.InternalTarget == ""
				if !ok || nextErr != nil || identityErr != nil && !legacyIdentity || value.Worktree != origin.Worktree ||
					value.Phase != fsPhasePreBase || value.Op != fsOpRename || value.Parent != parent || !validSource || value.Target != want ||
					(!legacyIdentity && (value.ParentDevice != previousTargetDevice || value.ParentInode != previousTargetInode)) ||
					value.ExpectedKind != "File" || value.ExpectedObject == "" || value.Outcome != "" {
					return errors.Join(errors.New("filesystem promotion chain is not continuous or deterministic"), nextErr, identityErr)
				}
				previous, previousAction = value.Target, value
			}
			continue
		}
		if origin.Phase != fsPhasePreBase || (origin.Op != fsOpCreateFile && origin.Op != fsOpCreateDirectory) ||
			origin.State != fsStateCompleted || origin.Outcome != "rolled_back" || origin.ExpectedDevice != 0 || origin.ExpectedInode != 0 {
			return errors.New("filesystem preserve chain has invalid creation origin")
		}
		rows, err := db.QueryContext(ctx, `SELECT cp.path FROM checkout_paths cp JOIN pending_checkouts pc ON pc.worktree=cp.worktree
			WHERE cp.worktree=? AND cp.create_action_id=? AND pc.apply_state='rolling_back'`, worktree, originID)
		if err != nil {
			return err
		}
		registered := 0
		for rows.Next() {
			var path string
			if err := rows.Scan(&path); err != nil {
				rows.Close()
				return err
			}
			parent, _ := splitFSActionPath(path)
			if parent == origin.Parent {
				registered++
			}
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
		if registered != 1 {
			return errors.New("filesystem preserve origin is not an exact rolling-back checkout path")
		}
		for attempt := 1; attempt <= len(chain); attempt++ {
			value, ok := chain[attempt]
			if !ok || value.Parent != origin.Parent || value.ParentDevice != origin.ParentDevice ||
				value.ParentInode != origin.ParentInode || value.Source != origin.Source || value.InternalSource != origin.Source ||
				value.ExpectedKind != origin.ExpectedKind {
				return errors.New("filesystem preserve chain is not contiguous or does not match its origin")
			}
			want, err := recoveryVisibleLeaf(value.Parent, originID, attempt)
			if err != nil || value.Target != want {
				return errors.Join(errors.New("filesystem preserve chain has invalid target"), err)
			}
			if value.Outcome == "collision" {
				if _, exists := chain[attempt+1]; !exists {
					return errors.New("filesystem preserve collision has no successor")
				}
			} else if attempt != len(chain) {
				return errors.New("filesystem preserve chain has an invalid successor")
			}
		}
	}
	for _, value := range values {
		if value.Outcome == "rolled_back" {
			if chain := chains[value.ActionID]; len(chain) == 0 {
				return errors.New("rolled-back filesystem creation has no preserve action")
			}
		}
	}
	return nil
}

func recoverFSActions(ctx context.Context, db *sql.DB, worktree string, root *openedWorktree, fault fsActionFault) error {
	if err := validatePendingCheckoutState(ctx, db, worktree); err != nil {
		return err
	}
	if err := bindFSJournalRoot(ctx, db, worktree, root); err != nil {
		return err
	}
	values, err := loadFSActions(ctx, db, worktree)
	if err != nil {
		return err
	}
	if err := validateFSActionJournal(ctx, db, worktree, values); err != nil {
		return err
	}
	if err := validatePromotionOwnership(ctx, db, worktree, values); err != nil {
		return err
	}
	intents := make([]fsAction, 0)
	for _, value := range values {
		if value.State == fsStateIntent {
			intents = append(intents, value)
		}
	}
	byID := make(map[string]fsAction, len(values))
	for _, value := range values {
		byID[value.ActionID] = value
	}
	rootID := func(value fsAction) string {
		if value.OriginActionID != "" {
			return value.OriginActionID
		}
		return value.ActionID
	}
	sort.SliceStable(intents, func(i, j int) bool {
		leftRoot, rightRoot := rootID(intents[i]), rootID(intents[j])
		if leftRoot == rightRoot {
			root := byID[leftRoot]
			if root.Op == fsOpRename && root.ExpectedObject != "" {
				return intents[i].Attempt > intents[j].Attempt
			}
			return intents[i].Order < intents[j].Order
		}
		return byID[leftRoot].Order < byID[rightRoot].Order
	})
	for _, value := range intents {
		if err := completeFSAction(ctx, db, root, value, fault); err != nil {
			return err
		}
	}
	return nil
}

func journalCreate(ctx context.Context, db *sql.DB, root *openedWorktree, worktree, checkoutPath, parentPath, name, kind string, fault fsActionFault) error {
	if err := bindFSJournalRoot(ctx, db, worktree, root); err != nil {
		return err
	}
	parent, err := openFSActionParent(root, parentPath, 0, 0)
	if err != nil {
		return err
	}
	var parentStat fscompat.Stat_t
	if err := fscompat.Fstat(int(parent.Fd()), &parentStat); err != nil {
		parent.Close()
		return err
	}
	if err := parent.Close(); err != nil {
		return err
	}
	op := fsOpCreateFile
	if kind == "Directory" {
		op = fsOpCreateDirectory
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	fail := func(err error) error { return errors.Join(err, tx.Rollback()) }
	var actionID, registeredName, registeredKind string
	if err := tx.QueryRowContext(ctx, `SELECT create_action_id, temp_name, type FROM checkout_paths
		WHERE worktree=? AND path=?`, worktree, checkoutPath).Scan(&actionID, &registeredName, &registeredKind); err != nil {
		return fail(err)
	}
	if registeredName != name || registeredKind != kind {
		return fail(errors.New("filesystem creation does not match fixed checkout path"))
	}
	if actionID == "" {
		actionID, err = newFSActionID()
		if err != nil {
			return fail(err)
		}
		var order int64
		if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(action_order), -1) + 1 FROM fs_actions WHERE worktree=?", worktree).Scan(&order); err != nil {
			return fail(err)
		}
		value := fsAction{Worktree: worktree, ActionID: actionID, Order: order, Phase: fsPhasePreBase, Op: op,
			Parent: parentPath, ParentDevice: uint64(parentStat.Dev), ParentInode: parentStat.Ino,
			Source: name, ExpectedKind: kind, InternalSource: name, State: fsStateIntent}
		if err := insertFSActionIntentWith(ctx, tx, value); err != nil {
			return fail(err)
		}
		result, err := tx.ExecContext(ctx, `UPDATE checkout_paths SET create_action_id=?
			WHERE worktree=? AND path=? AND temp_name=? AND type=? AND create_action_id=''`,
			actionID, worktree, checkoutPath, name, kind)
		if err != nil {
			return fail(err)
		}
		if changed, err := result.RowsAffected(); err != nil || changed != 1 {
			return fail(errors.Join(errors.New("bind filesystem creation to checkout path"), err))
		}
		if fault != nil {
			if err := fault("before_intent_commit", value); err != nil {
				return fail(err)
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		if fault != nil {
			if err := fault("after_intent_commit", value); err != nil {
				return err
			}
		}
		return completeFSAction(ctx, db, root, value, fault)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	values, err := loadFSActions(ctx, db, worktree)
	if err != nil {
		return err
	}
	for _, value := range values {
		if value.ActionID != actionID {
			continue
		}
		if value.OriginActionID != "" || value.Op != op || value.Parent != parentPath || value.Source != name ||
			value.ExpectedKind != kind || value.InternalSource != name || value.Phase != fsPhasePreBase {
			return errors.New("checkout path creation binding does not match its action")
		}
		if value.State == fsStateCompleted {
			return nil
		}
		return completeFSAction(ctx, db, root, value, fault)
	}
	return errors.New("checkout path creation binding has no action")
}

func completedFSCreateIdentity(ctx context.Context, db *sql.DB, worktree, checkoutPath string) (uint64, uint64, bool, error) {
	var device, inode uint64
	err := db.QueryRowContext(ctx, `SELECT action.expected_device, action.expected_inode
		FROM checkout_paths path JOIN fs_actions action ON action.worktree=path.worktree AND action.action_id=path.create_action_id
		WHERE path.worktree=? AND path.path=? AND action.state='completed' AND action.action_outcome=''`,
		worktree, checkoutPath).Scan(&device, &inode)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, false, nil
	}
	return device, inode, err == nil, err
}

func forgetCompletedFSCreate(ctx context.Context, db *sql.DB, worktree, parent, name, kind string) error {
	op := fsOpCreateFile
	if kind == "Directory" {
		op = fsOpCreateDirectory
	}
	_, err := db.ExecContext(ctx, `DELETE FROM fs_actions WHERE worktree = ? AND origin_action_id IS NULL AND parent_path = ? AND source_name = ?
		AND expected_kind = ? AND op = ? AND state = 'completed' AND action_outcome = ''`, worktree, parent, name, kind, op)
	return err
}

func journalMtime(ctx context.Context, db *sql.DB, root *openedWorktree, worktree, phase, parentPath,
	name, kind, mtime string, device, inode uint64, fault fsActionFault) error {
	parent, err := openFSActionParent(root, parentPath, 0, 0)
	if err != nil {
		return err
	}
	var parentStat fscompat.Stat_t
	if err := fscompat.Fstat(int(parent.Fd()), &parentStat); err != nil {
		parent.Close()
		return err
	}
	if err := parent.Close(); err != nil {
		return err
	}
	id, err := newFSActionID()
	if err != nil {
		return err
	}
	var order int64
	if err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(action_order), -1) + 1 FROM fs_actions WHERE worktree = ?", worktree).Scan(&order); err != nil {
		return err
	}
	internalSource := ""
	if validFSInternalName(name) {
		internalSource = name
	}
	return executeFSAction(ctx, db, root, fsAction{Worktree: worktree, ActionID: id, Order: order, Phase: phase,
		Op: fsOpMtime, Parent: parentPath, ParentDevice: uint64(parentStat.Dev), ParentInode: parentStat.Ino,
		Source: name, ExpectedKind: kind, ExpectedDevice: device, ExpectedInode: inode,
		ExpectedMtime: mtime, InternalSource: internalSource, State: fsStateIntent}, fault)
}

func journalRemove(ctx context.Context, db *sql.DB, root *openedWorktree, worktree, phase, parentPath,
	name, kind, internalSource, expectedObject, expectedMtime string, expectedSize int64, device, inode uint64, fault fsActionFault) error {
	parent, err := openFSActionParent(root, parentPath, 0, 0)
	if err != nil {
		return err
	}
	var parentStat fscompat.Stat_t
	if err := fscompat.Fstat(int(parent.Fd()), &parentStat); err != nil {
		parent.Close()
		return err
	}
	if err := parent.Close(); err != nil {
		return err
	}
	id, err := newFSActionID()
	if err != nil {
		return err
	}
	var order int64
	if err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(action_order), -1) + 1 FROM fs_actions WHERE worktree = ?", worktree).Scan(&order); err != nil {
		return err
	}
	op := fsOpUnlink
	if kind == "Directory" {
		op = fsOpRmdir
	}
	return executeFSAction(ctx, db, root, fsAction{Worktree: worktree, ActionID: id, Order: order, Phase: phase,
		Op: op, Parent: parentPath, ParentDevice: uint64(parentStat.Dev), ParentInode: parentStat.Ino,
		Source: name, ExpectedKind: kind, ExpectedDevice: device, ExpectedInode: inode,
		ExpectedObject: expectedObject, ExpectedSize: expectedSize, ExpectedMtime: expectedMtime,
		InternalSource: internalSource, State: fsStateIntent}, fault)
}

func splitFSActionPath(path string) (string, string) {
	if index := strings.LastIndexByte(path, '/'); index >= 0 {
		return path[:index], path[index+1:]
	}
	return "", path
}

func journalRename(ctx context.Context, db *sql.DB, root *openedWorktree, worktree, phase, parentPath,
	source, target, kind, internalSource, internalTarget string, device, inode uint64, fault fsActionFault) error {
	parent, err := openFSActionParent(root, parentPath, 0, 0)
	if err != nil {
		return err
	}
	var parentStat fscompat.Stat_t
	if err := fscompat.Fstat(int(parent.Fd()), &parentStat); err != nil {
		parent.Close()
		return err
	}
	if err := parent.Close(); err != nil {
		return err
	}
	id, err := newFSActionID()
	if err != nil {
		return err
	}
	var order int64
	if err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(action_order), -1) + 1 FROM fs_actions WHERE worktree = ?", worktree).Scan(&order); err != nil {
		return err
	}
	return executeFSAction(ctx, db, root, fsAction{Worktree: worktree, ActionID: id, Order: order, Phase: phase,
		Op: fsOpRename, Parent: parentPath, ParentDevice: uint64(parentStat.Dev), ParentInode: parentStat.Ino,
		Source: source, Target: target, ExpectedKind: kind, ExpectedDevice: device, ExpectedInode: inode,
		InternalSource: internalSource, InternalTarget: internalTarget, State: fsStateIntent}, fault)
}

func journalPromotion(ctx context.Context, db *sql.DB, root *openedWorktree, worktree, provenanceSource,
	sourceParent, source, target, internalSource, expectedObject, expectedMtime string, expectedSize int64,
	device, inode uint64, recovery *syncRecovery, fault fsActionFault) error {
	if recovery == nil {
		return errors.New("filesystem promotion requires an exact sync recovery owner")
	}
	parent, err := openFSActionParent(root, sourceParent, 0, 0)
	if err != nil {
		return err
	}
	var parentStat fscompat.Stat_t
	if err := fscompat.Fstat(int(parent.Fd()), &parentStat); err != nil {
		parent.Close()
		return err
	}
	if err := parent.Close(); err != nil {
		return err
	}
	if recovery.worktree != worktree || !recovery.completed ||
		(provenanceSource != recovery.path && !strings.HasPrefix(provenanceSource, recovery.path+"/")) {
		return errors.New("sync recovery does not own the promotion source")
	}
	targetParent, _, err := promotionTargetParent(ctx, db, root, worktree, target)
	if err != nil {
		return err
	}
	var targetParentStat fscompat.Stat_t
	if err := fscompat.Fstat(int(targetParent.Fd()), &targetParentStat); err != nil {
		targetParent.Close()
		return err
	}
	if err := targetParent.Close(); err != nil {
		return err
	}
	if err := bindFSJournalRoot(ctx, db, worktree, root); err != nil {
		return err
	}
	id, err := newFSActionID()
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	fail := func(err error) error { return errors.Join(err, tx.Rollback()) }
	var order int64
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(action_order), -1) + 1 FROM fs_actions WHERE worktree=?", worktree).Scan(&order); err != nil {
		return fail(err)
	}
	value := fsAction{Worktree: worktree, ActionID: id, Order: order, Phase: fsPhasePreBase, Op: fsOpRename,
		Parent: sourceParent, ParentDevice: uint64(parentStat.Dev), ParentInode: parentStat.Ino,
		Source: source, Target: target, ExpectedKind: "File", ExpectedDevice: device, ExpectedInode: inode,
		ExpectedObject: expectedObject, ExpectedSize: expectedSize, ExpectedMtime: expectedMtime,
		InternalSource: internalSource,
		InternalTarget: encodePromotionTargetParent(uint64(targetParentStat.Dev), targetParentStat.Ino), State: fsStateIntent}
	if err := insertFSActionIntentWith(ctx, tx, value); err != nil {
		return fail(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO sync_recovery_promotions(worktree,recovery_path,source_path,current_action_id,rollback_action_id)
		VALUES(?,?,?,?, '')`, worktree, recovery.path, provenanceSource, id); err != nil {
		return fail(fmt.Errorf("bind promotion action to sync recovery source: %w", err))
	}
	journal, err := loadFSActionsWith(ctx, tx, worktree)
	if err != nil {
		return fail(err)
	}
	if err := validateFSActionJournal(ctx, tx, worktree, journal); err != nil {
		return fail(err)
	}
	if err := validatePromotionOwnership(ctx, tx, worktree, journal); err != nil {
		return fail(err)
	}
	if fault != nil {
		if err := fault("before_intent_commit", value); err != nil {
			return fail(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if fault != nil {
		if err := fault("after_intent_commit", value); err != nil {
			return err
		}
	}
	return completeFSAction(ctx, db, root, value, fault)
}

func promotionChainNamingSeed(ctx context.Context, db fsActionQueryer, worktree, rootID string, journal []fsAction) (string, string, error) {
	byID := make(map[string]fsAction, len(journal))
	for _, action := range journal {
		byID[action.ActionID] = action
	}
	links, err := loadSyncRecoveryPromotionsWith(ctx, db, worktree)
	if err != nil {
		return "", "", err
	}
	promotions, err := loadConflictPromotionsWith(ctx, db, worktree)
	if err != nil {
		return "", "", err
	}
	bySource := make(map[string]_conflictPromotion, len(promotions))
	for _, promotion := range promotions {
		bySource[promotion.source] = promotion
	}
	for _, link := range links {
		current, ok := byID[link.currentActionID]
		if !ok {
			continue
		}
		currentRoot := current.ActionID
		if current.OriginActionID != "" {
			currentRoot = current.OriginActionID
		}
		if currentRoot == rootID {
			promotion, ok := bySource[link.sourcePath]
			if !ok {
				return "", "", errors.New("promotion chain has no exact NamingSeedCommitId owner")
			}
			return promotion.namingSeed, promotion.source, nil
		}
	}
	return "", "", nil
}

func ensurePromotionFallbackRoot(ctx context.Context, db *sql.DB, root *openedWorktree, worktree, rootID string,
	fault fsActionFault) (string, uint64, uint64, error) {
	if !validFSActionID(rootID) {
		return "", 0, 0, errors.New("invalid promotion root for fallback directory")
	}
	if _, err := root.directory.Seek(0, 0); err != nil {
		return "", 0, 0, err
	}
	names, err := root.directory.Readdirnames(-1)
	if err != nil {
		return "", 0, 0, err
	}
	exact := make(map[string]bool, len(names))
	folded := make(map[string]bool, len(names))
	for _, name := range names {
		exact[name] = true
		folded[cases.Fold().String(name)] = true
	}
	for ordinal := 1; ordinal <= _conflictMaxOrdinal; ordinal++ {
		name := _fallbackConflictRoot
		if ordinal > 1 {
			name += " " + strconv.Itoa(ordinal)
		}
		if exact[name] {
			var stat fscompat.Stat_t
			if err := fscompat.Fstatat(int(root.directory.Fd()), name, &stat, fscompat.AT_SYMLINK_NOFOLLOW); err != nil {
				return "", 0, 0, err
			}
			if stat.Mode&fscompat.S_IFMT == fscompat.S_IFDIR && uint64(stat.Dev) == root.device {
				return name, uint64(stat.Dev), stat.Ino, nil
			}
			continue
		}
		if folded[cases.Fold().String(name)] {
			continue
		}
		values, err := loadFSActions(ctx, db, worktree)
		if err != nil {
			return "", 0, 0, err
		}
		owner := fsPromotionFallbackOwnerPrefix + rootID
		var creation fsAction
		for _, value := range values {
			if value.InternalTarget == owner && value.Source == name {
				creation = value
				break
			}
		}
		if creation.ActionID == "" {
			id, err := newFSActionID()
			if err != nil {
				return "", 0, 0, err
			}
			var order int64
			if err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(action_order), -1) + 1 FROM fs_actions WHERE worktree=?", worktree).Scan(&order); err != nil {
				return "", 0, 0, err
			}
			creation = fsAction{Worktree: worktree, ActionID: id, Order: order, Phase: fsPhasePreBase,
				Op: fsOpCreateDirectory, ParentDevice: root.device, ParentInode: root.inode, Source: name,
				ExpectedKind: "Directory", InternalTarget: owner, State: fsStateIntent}
			if err := executeFSAction(ctx, db, root, creation, fault); err != nil {
				return "", 0, 0, err
			}
		} else if creation.State == fsStateIntent {
			if err := completeFSAction(ctx, db, root, creation, fault); err != nil {
				return "", 0, 0, err
			}
		}
		var device, inode uint64
		var outcome string
		if err := db.QueryRowContext(ctx, `SELECT expected_device,expected_inode,action_outcome FROM fs_actions
			WHERE worktree=? AND action_id=? AND state='completed'`, worktree, creation.ActionID).Scan(&device, &inode, &outcome); err != nil {
			return "", 0, 0, err
		}
		if outcome == "collision" {
			continue
		}
		parent, err := openFSActionParent(root, name, device, inode)
		if err != nil {
			return "", 0, 0, err
		}
		return name, device, inode, parent.Close()
	}
	return "", 0, 0, errors.New("fallback conflict root collision sequence exhausted")
}

func journalLatePromotion(ctx context.Context, db *sql.DB, root *openedWorktree, link *syncRecoveryPromotion,
	current fsAction, parentDevice, parentInode uint64, target, expectedObject, expectedMtime string,
	expectedSize int64, device, inode uint64, fault fsActionFault) (fsAction, error) {
	if link == nil || link.worktree != current.Worktree || link.currentActionID != current.ActionID ||
		current.State != fsStateCompleted || current.Op != fsOpRename || current.ExpectedObject == "" {
		return fsAction{}, errors.New("invalid late promotion linkage")
	}
	rootID := current.ActionID
	if current.OriginActionID != "" {
		rootID = current.OriginActionID
	}
	parent, source := splitFSActionPath(current.Target)
	if err := bindFSJournalRoot(ctx, db, current.Worktree, root); err != nil {
		return fsAction{}, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fsAction{}, err
	}
	fail := func(err error) (fsAction, error) { return fsAction{}, errors.Join(err, tx.Rollback()) }
	journal, err := loadFSActionsWith(ctx, tx, current.Worktree)
	if err != nil {
		return fail(err)
	}
	if err := validateFSActionJournal(ctx, tx, current.Worktree, journal); err != nil {
		return fail(err)
	}
	if err := validatePromotionOwnership(ctx, tx, current.Worktree, journal); err != nil {
		return fail(err)
	}
	seed, sourcePath, err := promotionChainNamingSeed(ctx, tx, current.Worktree, rootID, journal)
	if err != nil {
		return fail(err)
	}
	tail, attempt := current.Target, 1
	for _, value := range journal {
		if value.OriginActionID == rootID && value.Attempt >= attempt {
			tail, attempt = value.Target, value.Attempt+1
		}
	}
	want, err := nextPromotionChainPath(tail, seed, sourcePath)
	if err != nil {
		return fail(err)
	}
	targetParentDevice, targetParentInode := parentDevice, parentInode
	wantParent, _ := splitFSActionPath(want)
	tailParent, _ := splitFSActionPath(tail)
	if wantParent == _fallbackConflictRoot && wantParent != tailParent {
		if err := tx.Rollback(); err != nil {
			return fsAction{}, err
		}
		rootName, device, inode, err := ensurePromotionFallbackRoot(ctx, db, root, current.Worktree, rootID, fault)
		if err != nil {
			return fsAction{}, err
		}
		_, originalLeaf := splitFSActionPath(sourcePath)
		leaf, err := _fallbackConflictName(originalLeaf, rootName, seed, 1)
		if err != nil {
			return fsAction{}, err
		}
		want = rootName + "/" + leaf
		targetParentDevice, targetParentInode = device, inode
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			return fsAction{}, err
		}
		fail = func(err error) (fsAction, error) { return fsAction{}, errors.Join(err, tx.Rollback()) }
		journal, err = loadFSActionsWith(ctx, tx, current.Worktree)
		if err != nil {
			return fail(err)
		}
	} else if wantParent != parent {
		return fail(errors.New("late promotion target parent changed outside root fallback transition"))
	}
	if target == "" {
		target = want
	}
	if target != want {
		return fail(errors.New("late promotion target is not the strict immediate successor"))
	}
	var successor fsAction
	for _, value := range journal {
		if value.OriginActionID == rootID && value.Attempt == attempt {
			successor = value
			break
		}
	}
	if successor.ActionID == "" {
		id, err := newFSActionID()
		if err != nil {
			return fail(err)
		}
		var order int64
		if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(action_order), -1) + 1 FROM fs_actions WHERE worktree=?",
			current.Worktree).Scan(&order); err != nil {
			return fail(err)
		}
		successor = fsAction{Worktree: current.Worktree, ActionID: id, OriginActionID: rootID,
			Attempt: attempt, Order: order, Phase: fsPhasePreBase, Op: fsOpRename,
			Parent: parent, ParentDevice: parentDevice, ParentInode: parentInode, Source: source, Target: target,
			ExpectedKind: "File", ExpectedDevice: device, ExpectedInode: inode, ExpectedObject: expectedObject,
			ExpectedSize: expectedSize, ExpectedMtime: expectedMtime,
			InternalTarget: encodePromotionTargetParent(targetParentDevice, targetParentInode), State: fsStateIntent}
		if err := insertFSActionIntentWith(ctx, tx, successor); err != nil {
			return fail(err)
		}
	} else if successor.Parent != parent || successor.ParentDevice != parentDevice || successor.ParentInode != parentInode ||
		successor.Source != source || successor.Target != target ||
		successor.InternalTarget != encodePromotionTargetParent(targetParentDevice, targetParentInode) || successor.ExpectedDevice != device ||
		successor.ExpectedInode != inode || successor.ExpectedObject != expectedObject ||
		successor.ExpectedSize != expectedSize || successor.ExpectedMtime != expectedMtime {
		return fail(errors.New("late promotion successor does not match its exact chain slot"))
	}
	args := append([]any{successor.ActionID}, syncRecoveryPromotionCASArgs(*link)...)
	result, err := tx.ExecContext(ctx, "UPDATE sync_recovery_promotions SET current_action_id=? WHERE "+_syncRecoveryPromotionCAS, args...)
	if err != nil {
		return fail(err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return fail(errors.Join(errors.New("advance late promotion source linkage"), err))
	}
	if fault != nil {
		if err := fault("before_intent_commit", successor); err != nil {
			return fail(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fsAction{}, err
	}
	link.currentActionID = successor.ActionID
	if fault != nil {
		if err := fault("after_intent_commit", successor); err != nil {
			return fsAction{}, err
		}
	}
	if successor.State == fsStateIntent {
		if err := completeFSAction(ctx, db, root, successor, fault); err != nil {
			return fsAction{}, err
		}
		successor.State = fsStateCompleted
	}
	return successor, nil
}

func journalRestorePromotion(ctx context.Context, db *sql.DB, root *openedWorktree, recovery syncRecovery,
	link *syncRecoveryPromotion, current fsAction, target string, fault fsActionFault) error {
	if link == nil || link.worktree != recovery.worktree || link.recoveryPath != recovery.path ||
		link.currentActionID != current.ActionID || link.rollbackActionID != "" || current.State != fsStateCompleted ||
		current.Op != fsOpRename || current.ExpectedObject == "" {
		return errors.New("invalid promotion restore linkage")
	}
	sourceParentPath, sourceLeaf := splitFSActionPath(current.Target)
	sourceParent, sourceLeafOpened, err := promotionTargetParent(ctx, db, root, recovery.worktree, current.Target)
	if err != nil {
		return err
	}
	if sourceLeafOpened != sourceLeaf {
		sourceParent.Close()
		return errors.New("promotion restore source path changed during resolution")
	}
	var parentStat fscompat.Stat_t
	if err := fscompat.Fstat(int(sourceParent.Fd()), &parentStat); err != nil {
		sourceParent.Close()
		return err
	}
	id, err := newFSActionID()
	if err != nil {
		sourceParent.Close()
		return err
	}
	parentMtime, err := restorePromotionParentMtime(ctx, db, recovery, link.sourcePath)
	if err != nil {
		sourceParent.Close()
		return err
	}
	value := fsAction{Worktree: recovery.worktree, ActionID: id, Phase: fsPhaseRollback,
		Op: fsOpRestorePromotion, Parent: sourceParentPath, ParentDevice: uint64(parentStat.Dev), ParentInode: parentStat.Ino,
		Source: sourceLeaf, Target: target, ExpectedKind: "File", ExpectedDevice: current.ExpectedDevice,
		ExpectedInode: current.ExpectedInode, ExpectedMtime: parentMtime, State: fsStateIntent}
	sourceExists, sourceMatches, stateErr := actionPathState(sourceParent, sourceLeaf, value)
	closeErr := sourceParent.Close()
	if stateErr != nil || closeErr != nil || !sourceExists || !sourceMatches {
		return errors.Join(errors.New("promotion restore source no longer has its captured identity"), stateErr, closeErr)
	}
	if err := bindFSJournalRoot(ctx, db, recovery.worktree, root); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	fail := func(err error) error { return errors.Join(err, tx.Rollback()) }
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(action_order), -1) + 1 FROM fs_actions WHERE worktree=?",
		recovery.worktree).Scan(&value.Order); err != nil {
		return fail(err)
	}
	if err := insertFSActionIntentWith(ctx, tx, value); err != nil {
		return fail(err)
	}
	args := append([]any{id}, syncRecoveryPromotionCASArgs(*link)...)
	result, err := tx.ExecContext(ctx, "UPDATE sync_recovery_promotions SET rollback_action_id=? WHERE "+_syncRecoveryPromotionCAS, args...)
	if err != nil {
		return fail(err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return fail(errors.Join(errors.New("bind promotion restore action to exact source linkage"), err))
	}
	link.rollbackActionID = id
	journal, err := loadFSActionsWith(ctx, tx, recovery.worktree)
	if err != nil {
		return fail(err)
	}
	if err := validateFSActionJournal(ctx, tx, recovery.worktree, journal); err != nil {
		return fail(err)
	}
	if err := validatePromotionOwnership(ctx, tx, recovery.worktree, journal); err != nil {
		return fail(err)
	}
	if fault != nil {
		if err := fault("before_intent_commit", value); err != nil {
			return fail(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if fault != nil {
		if err := fault("after_intent_commit", value); err != nil {
			return err
		}
	}
	return completeFSAction(ctx, db, root, value, fault)
}

func promotionTargetParent(ctx context.Context, db *sql.DB, root *openedWorktree, worktree, target string,
	encodedIdentity ...string) (*os.File, string, error) {
	parentPath, leaf := splitFSActionPath(target)
	if len(encodedIdentity) != 0 && encodedIdentity[0] != "" {
		device, inode, err := decodePromotionTargetParent(encodedIdentity[0])
		if err != nil {
			return nil, "", err
		}
		parent, err := openFSActionParent(root, parentPath, device, inode)
		return parent, leaf, err
	}
	if parentPath == "" {
		parent, err := openFSActionParent(root, "", root.device, root.inode)
		return parent, leaf, err
	}
	var device, inode uint64
	var completed bool
	err := db.QueryRowContext(ctx, `SELECT target_device, target_inode, completed FROM checkout_paths
		WHERE worktree=? AND path=? AND type='Directory'`, worktree, parentPath).Scan(&device, &inode, &completed)
	if err != nil || !completed || device == 0 || inode == 0 {
		return nil, "", errors.Join(errors.New("filesystem promotion target parent is not durably installed"), err)
	}
	parent, err := openFSActionParent(root, parentPath, device, inode)
	return parent, leaf, err
}

func openRestorePromotionTargetParent(root *openedWorktree, recovery syncRecovery, target string) (*os.File, string, error) {
	if recovery.worktree == "" || !recovery.completed ||
		!strings.HasPrefix(recovery.name, syncRecoveryPrefix) || target != recovery.name && !strings.HasPrefix(target, recovery.name+"/") {
		return nil, "", errors.New("filesystem restore promotion has no exact hidden recovery target")
	}
	components := strings.Split(target, "/")
	if len(components) == 1 {
		if recovery.kind != "File" || target != recovery.name {
			return nil, "", errors.New("filesystem restore promotion target does not match its recovery type")
		}
		parent, err := openFSActionParent(root, "", root.device, root.inode)
		return parent, components[0], err
	}
	if recovery.kind != "Directory" || components[0] != recovery.name {
		return nil, "", errors.New("filesystem restore promotion descendant has no directory recovery owner")
	}
	fd, err := fscompat.Openat(int(root.directory.Fd()), recovery.name,
		fscompat.O_RDONLY|fscompat.O_DIRECTORY|fscompat.O_NOFOLLOW|fscompat.O_CLOEXEC, 0)
	if err != nil {
		return nil, "", err
	}
	var stat fscompat.Stat_t
	if err := fscompat.Fstat(fd, &stat); err != nil || !statMatchesRecovery(stat, recovery) {
		fscompat.Close(fd)
		return nil, "", errors.Join(errors.New("filesystem restore promotion recovery root changed identity"), err)
	}
	current := fd
	for _, component := range components[1 : len(components)-1] {
		next, openErr := fscompat.Openat(current, component,
			fscompat.O_RDONLY|fscompat.O_DIRECTORY|fscompat.O_NOFOLLOW|fscompat.O_CLOEXEC, 0)
		fscompat.Close(current)
		if openErr != nil {
			return nil, "", openErr
		}
		current = next
	}
	return os.NewFile(uintptr(current), target), components[len(components)-1], nil
}

func completeRestorePromotionAction(ctx context.Context, db *sql.DB, root *openedWorktree, value fsAction, fault fsActionFault) error {
	sourceParent, err := openFSActionParent(root, value.Parent, value.ParentDevice, value.ParentInode)
	if err != nil {
		return err
	}
	defer sourceParent.Close()
	var recovery syncRecovery
	err = db.QueryRowContext(ctx, `SELECT r.worktree,r.path,r.recovery_name,r.tombstone_name,r.type,r.object_id,r.canonical_mtime,
		r.size,r.device,r.inode,r.completed FROM sync_recovery_promotions p JOIN sync_recoveries r
		ON r.worktree=p.worktree AND r.path=p.recovery_path
		WHERE p.worktree=? AND p.rollback_action_id=? AND r.recovery_name=?`,
		value.Worktree, value.ActionID, strings.Split(value.Target, "/")[0]).Scan(&recovery.worktree, &recovery.path, &recovery.name,
		&recovery.tombstone, &recovery.kind, &recovery.id, &recovery.mtime, &recovery.size, &recovery.device,
		&recovery.inode, &recovery.completed)
	if err != nil {
		return errors.Join(errors.New("filesystem restore promotion recovery owner is absent"), err)
	}
	targetParent, targetLeaf, err := openRestorePromotionTargetParent(root, recovery, value.Target)
	if err != nil {
		return err
	}
	defer targetParent.Close()
	sourceExists, sourceMatches, err := actionPathState(sourceParent, value.Source, value)
	if err != nil {
		return err
	}
	targetExists, targetMatches, err := actionPathState(targetParent, targetLeaf, value)
	if err != nil {
		return err
	}
	changed := false
	switch {
	case sourceExists && sourceMatches && !targetExists:
		if fault != nil {
			if err := fault("before_action", value); err != nil {
				return err
			}
		}
		if err := renameNoReplace(int(sourceParent.Fd()), value.Source, int(targetParent.Fd()), targetLeaf); err != nil {
			if errors.Is(err, fscompat.EEXIST) {
				return completeRestorePromotionAction(ctx, db, root, value, fault)
			}
			return fmt.Errorf("execute journaled promotion restore: %w", err)
		}
		changed = true
	case !sourceExists && targetExists && targetMatches:
		// The atomic cross-parent rename happened before restart.
	default:
		return errors.New("filesystem restore promotion has ambiguous or mismatched source/target state")
	}
	if fault != nil && changed {
		if err := fault("after_action", value); err != nil {
			return err
		}
	}
	if value.ExpectedMtime != "" {
		if err := setOpenFileMtime(targetParent, value.ExpectedMtime); err != nil {
			return fmt.Errorf("restore promotion target parent mtime: %w", err)
		}
	}
	if err := sourceParent.Sync(); err != nil {
		return fmt.Errorf("sync filesystem restore promotion source parent: %w", err)
	}
	if err := targetParent.Sync(); err != nil {
		return fmt.Errorf("sync filesystem restore promotion target parent: %w", err)
	}
	if fault != nil {
		if err := fault("after_parent_sync", value); err != nil {
			return err
		}
	}
	result, err := db.ExecContext(ctx, `UPDATE fs_actions SET state='completed' WHERE worktree=? AND action_id=? AND state='intent'`,
		value.Worktree, value.ActionID)
	if err != nil {
		return err
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		return errors.Join(errors.New("filesystem restore promotion completion did not update one Intent"), err)
	}
	if fault != nil {
		return fault("after_completed", value)
	}
	return nil
}

func verifyPromotionSource(parent *os.File, value fsAction) error {
	file, info, err := openScannableAt(parent, value.Source, value.Parent+"/"+value.Source)
	if err != nil {
		return err
	}
	defer file.Close()
	mtime := canonicalProtocolMtime(info.ModTime())
	snapshot := worktreeSnapshot{blocks: make(map[string]blockSource)}
	id, err := scanRegularFile(file, value.Source, info, &snapshot)
	if err != nil || id != value.ExpectedObject || info.Size() != value.ExpectedSize || mtime != value.ExpectedMtime {
		return errors.Join(errors.New("filesystem promotion source content changed"), err)
	}
	return nil
}

func completePromotionAction(ctx context.Context, db *sql.DB, root *openedWorktree, value fsAction, fault fsActionFault) error {
	sourceParent, err := openFSActionParent(root, value.Parent, value.ParentDevice, value.ParentInode)
	if err != nil {
		return err
	}
	defer sourceParent.Close()
	targetParent, targetLeaf, err := promotionTargetParent(ctx, db, root, value.Worktree, value.Target, value.InternalTarget)
	if err != nil {
		return err
	}
	defer targetParent.Close()
	sourceExists, sourceMatches, err := actionPathState(sourceParent, value.Source, value)
	if err != nil {
		return err
	}
	targetExists, targetMatches, err := actionPathState(targetParent, targetLeaf, value)
	if err != nil {
		return err
	}
	if targetExists && targetMatches {
		aliases, err := parentCasefoldAliases(targetParent, targetLeaf)
		if err != nil {
			return err
		}
		for _, alias := range aliases {
			if err := preservePromotionCollision(ctx, db, root, value, targetParent, alias, fault); err != nil {
				return err
			}
		}
		if len(aliases) != 0 {
			return completePromotionAction(ctx, db, root, value, fault)
		}
	}
	changed := false
	switch {
	case sourceExists && sourceMatches && targetExists && !targetMatches:
		if err := preservePromotionCollision(ctx, db, root, value, targetParent, targetLeaf, fault); err != nil {
			return err
		}
		return completePromotionAction(ctx, db, root, value, fault)
	case sourceExists && sourceMatches && !targetExists:
		if err := verifyPromotionSource(sourceParent, value); err != nil {
			return err
		}
		if fault != nil {
			if err := fault("before_action", value); err != nil {
				return err
			}
		}
		if err := renameNoReplace(int(sourceParent.Fd()), value.Source, int(targetParent.Fd()), targetLeaf); err != nil {
			if errors.Is(err, fscompat.EEXIST) {
				return completePromotionAction(ctx, db, root, value, fault)
			}
			return fmt.Errorf("execute journaled promotion: %w", err)
		}
		changed = true
	case !sourceExists && targetExists && targetMatches:
		// The atomic cross-parent rename happened before restart.
	default:
		return errors.New("filesystem promotion has ambiguous or mismatched source/target state")
	}
	if fault != nil && changed {
		if err := fault("after_action", value); err != nil {
			return err
		}
	}
	if err := sourceParent.Sync(); err != nil {
		return fmt.Errorf("sync filesystem promotion source parent: %w", err)
	}
	if err := targetParent.Sync(); err != nil {
		return fmt.Errorf("sync filesystem promotion target parent: %w", err)
	}
	if fault != nil {
		if err := fault("after_parent_sync", value); err != nil {
			return err
		}
	}
	result, err := db.ExecContext(ctx, `UPDATE fs_actions SET state='completed' WHERE worktree=? AND action_id=? AND state='intent'`,
		value.Worktree, value.ActionID)
	if err != nil {
		return err
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		return errors.Join(errors.New("filesystem promotion completion did not update one Intent"), err)
	}
	if fault != nil {
		return fault("after_completed", value)
	}
	return nil
}

func preservePromotionCollision(ctx context.Context, db *sql.DB, root *openedWorktree, origin fsAction,
	targetParent *os.File, sourceLeaf string, fault fsActionFault) error {
	parentPath, _ := splitFSActionPath(origin.Target)
	sourcePath := sourceLeaf
	if parentPath != "" {
		sourcePath = parentPath + "/" + sourceLeaf
	}
	file, info, err := openScannableAt(targetParent, sourceLeaf, sourcePath)
	if err != nil {
		return err
	}
	stat, statErr := statOfOpenFile(file)
	ok := statErr == nil
	snapshot := worktreeSnapshot{blocks: make(map[string]blockSource)}
	id, scanErr := scanRegularFile(file, sourcePath, info, &snapshot)
	closeErr := file.Close()
	if scanErr != nil || closeErr != nil || !ok {
		return errors.Join(scanErr, closeErr, errors.New("inspect promotion collision identity"))
	}
	rootID := origin.ActionID
	if origin.OriginActionID != "" {
		rootID = origin.OriginActionID
	}
	mtime := canonicalProtocolMtime(info.ModTime())
	values, err := loadFSActions(ctx, db, origin.Worktree)
	if err != nil {
		return err
	}
	if err := validateFSActionJournal(ctx, db, origin.Worktree, values); err != nil {
		return err
	}
	if err := validatePromotionOwnership(ctx, db, origin.Worktree, values); err != nil {
		return err
	}
	rootTarget, tailTarget, attempt := "", "", 1
	for _, value := range values {
		if value.ActionID == rootID {
			rootTarget, tailTarget = value.Target, value.Target
		}
	}
	for _, value := range values {
		if value.OriginActionID == rootID && value.Attempt >= attempt {
			attempt, tailTarget = value.Attempt+1, value.Target
		}
	}
	seed, sourcePath, err := promotionChainNamingSeed(ctx, db, origin.Worktree, rootID, values)
	if err != nil {
		return err
	}
	var parentStat fscompat.Stat_t
	if err := fscompat.Fstat(int(targetParent.Fd()), &parentStat); err != nil {
		return err
	}
	successor, err := nextPromotionChainPath(tailTarget, seed, sourcePath)
	if err != nil {
		return err
	}
	targetParentDevice, targetParentInode := uint64(parentStat.Dev), parentStat.Ino
	successorParent, _ := splitFSActionPath(successor)
	if successorParent == _fallbackConflictRoot && successorParent != parentPath {
		rootName, device, inode, err := ensurePromotionFallbackRoot(ctx, db, root, origin.Worktree, rootID, fault)
		if err != nil {
			return err
		}
		_, originalLeaf := splitFSActionPath(sourcePath)
		leaf, err := _fallbackConflictName(originalLeaf, rootName, seed, 1)
		if err != nil {
			return err
		}
		successor = rootName + "/" + leaf
		targetParentDevice, targetParentInode = device, inode
	}
	if err := validateRootPromotionTarget(rootTarget, successor, seed, sourcePath); err != nil {
		return err
	}
	for _, existing := range values {
		if existing.OriginActionID != rootID || existing.Attempt != attempt {
			continue
		}
		if existing.Parent != parentPath || existing.Source != sourceLeaf || existing.Target != successor ||
			existing.InternalTarget != encodePromotionTargetParent(targetParentDevice, targetParentInode) ||
			existing.ExpectedDevice != uint64(stat.Dev) || existing.ExpectedInode != stat.Ino ||
			existing.ExpectedObject != id || existing.ExpectedSize != info.Size() || existing.ExpectedMtime != mtime {
			return errors.New("promotion collision successor does not match its exact chain slot")
		}
		if existing.State == fsStateCompleted {
			return nil
		}
		return completeFSAction(ctx, db, root, existing, fault)
	}
	actionID, err := newFSActionID()
	if err != nil {
		return err
	}
	var order int64
	if err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(action_order), -1) + 1 FROM fs_actions WHERE worktree=?", origin.Worktree).Scan(&order); err != nil {
		return err
	}
	return executeFSAction(ctx, db, root, fsAction{Worktree: origin.Worktree, ActionID: actionID,
		OriginActionID: rootID, Attempt: attempt, Order: order, Phase: fsPhasePreBase, Op: fsOpRename,
		Parent: parentPath, ParentDevice: uint64(parentStat.Dev), ParentInode: parentStat.Ino,
		Source: sourceLeaf, Target: successor, ExpectedKind: "File", ExpectedDevice: uint64(stat.Dev),
		ExpectedInode: stat.Ino, ExpectedObject: id, ExpectedSize: info.Size(), ExpectedMtime: mtime,
		InternalTarget: encodePromotionTargetParent(targetParentDevice, targetParentInode), State: fsStateIntent}, fault)
}

func assertNoIncompletePreBase(ctx context.Context, tx *sql.Tx, worktree string) error {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fs_actions
		WHERE worktree = ? AND phase = 'pre_base' AND state <> 'completed'`, worktree).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return errors.New("cannot advance Sync Base with incomplete pre-base filesystem actions")
	}
	return nil
}
