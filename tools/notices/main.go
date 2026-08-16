// Command notices generates the third-party license material shipped with Filecloud releases.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

type packageInfo struct {
	Module *moduleInfo
}

type moduleInfo struct {
	Path    string
	Version string
	Dir     string
	Main    bool
	Replace *moduleInfo
}

type noticeModule struct {
	path         string
	version      string
	dir          string
	licenseFiles []string
}

type releaseTarget struct {
	goos   string
	goarch string
}

func main() {
	output := flag.String("output", "dist/licenses", "output directory")
	target := flag.String("target", "", "release target (linux/amd64, darwin/arm64, or windows/amd64; empty selects all)")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: go run ./tools/notices --output directory [--target goos/goarch]")
		os.Exit(2)
	}
	if err := generate(*output, *target); err != nil {
		fmt.Fprintln(os.Stderr, "notices:", err)
		os.Exit(1)
	}
}

func generate(output, selectedTarget string) error {
	targets, err := selectedTargets(selectedTarget)
	if err != nil {
		return err
	}
	root, err := moduleRoot()
	if err != nil {
		return err
	}
	modules := make(map[string]noticeModule)
	for _, target := range targets {
		if err := collectTarget(target, root, modules); err != nil {
			return err
		}
	}
	cleanOutput := filepath.Clean(output)
	if cleanOutput == "." || cleanOutput == string(filepath.Separator) {
		return errors.New("output must be a dedicated directory")
	}
	if err := os.MkdirAll(filepath.Dir(cleanOutput), 0o755); err != nil {
		return fmt.Errorf("create output parent: %w", err)
	}
	if err := os.Mkdir(cleanOutput, 0o755); err != nil {
		return fmt.Errorf("create output directory (must not already exist): %w", err)
	}
	licensesDir := filepath.Join(cleanOutput, "third_party_licenses")
	if err := os.Mkdir(licensesDir, 0o755); err != nil {
		return fmt.Errorf("create licenses directory: %w", err)
	}

	values := make([]noticeModule, 0, len(modules))
	for _, module := range modules {
		files, err := findLicenseFiles(module.dir)
		if err != nil {
			return fmt.Errorf("find license for %s@%s: %w", module.path, module.version, err)
		}
		module.licenseFiles = files
		values = append(values, module)
	}
	slices.SortFunc(values, func(a, b noticeModule) int { return strings.Compare(a.path, b.path) })

	var notices strings.Builder
	notices.WriteString("# Third-Party Notices\n\n")
	if selectedTarget == "" {
		notices.WriteString("Generated from the packages compiled into Filecloud for `linux/amd64`, `darwin/arm64`, and `windows/amd64`.\n\n")
	} else {
		fmt.Fprintf(&notices, "Generated on the native release runner from the packages compiled into Filecloud for `%s`.\n\n", selectedTarget)
	}
	notices.WriteString("| Module | Version | License files |\n|---|---|---|\n")
	for _, module := range values {
		directory := moduleDirectory(module)
		for _, name := range module.licenseFiles {
			if err := copyFile(filepath.Join(module.dir, name), filepath.Join(licensesDir, directory, name)); err != nil {
				return fmt.Errorf("copy %s license %s: %w", module.path, name, err)
			}
		}
		fmt.Fprintf(&notices, "| `%s` | `%s` | `%s` |\n", module.path, module.version, strings.Join(module.licenseFiles, "`, `"))
	}
	if err := os.WriteFile(filepath.Join(cleanOutput, "THIRD_PARTY_NOTICES.md"), []byte(notices.String()), 0o644); err != nil {
		return fmt.Errorf("write notices: %w", err)
	}
	return nil
}

func selectedTargets(selected string) ([]releaseTarget, error) {
	targets := []releaseTarget{
		{goos: "linux", goarch: "amd64"},
		{goos: "darwin", goarch: "arm64"},
		{goos: "windows", goarch: "amd64"},
	}
	if selected == "" {
		return targets, nil
	}
	for _, target := range targets {
		if selected == target.goos+"/"+target.goarch {
			return []releaseTarget{target}, nil
		}
	}
	return nil, fmt.Errorf("unsupported release target %q", selected)
}

func moduleRoot() (string, error) {
	output, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		return "", fmt.Errorf("locate Go module: %w", err)
	}
	module := strings.TrimSpace(string(output))
	if module == "" || module == os.DevNull {
		return "", errors.New("not inside a Go module")
	}
	return filepath.Dir(module), nil
}

func collectTarget(target releaseTarget, root string, modules map[string]noticeModule) error {
	name := target.goos + "/" + target.goarch
	command := exec.Command("go", "list", "-deps", "-json", "./cmd/filecloud")
	command.Dir = root
	command.Env = append(os.Environ(), "GOOS="+target.goos, "GOARCH="+target.goarch, "CGO_ENABLED=0")
	stdout, err := command.StdoutPipe()
	if err != nil {
		return fmt.Errorf("open go list output for %s: %w", name, err)
	}
	var stderr strings.Builder
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		return fmt.Errorf("start go list for %s: %w", name, err)
	}
	decoder := json.NewDecoder(stdout)
	for {
		var info packageInfo
		if err := decoder.Decode(&info); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return errors.Join(fmt.Errorf("decode go list for %s: %w", name, err), terminate(command))
		}
		module := info.Module
		if module == nil || module.Main {
			continue
		}
		if module.Replace != nil {
			module = module.Replace
		}
		if module.Path == "" || module.Version == "" || module.Dir == "" {
			return errors.Join(fmt.Errorf("dependency for %s has incomplete module metadata", name), terminate(command))
		}
		modules[module.Path+"\x00"+module.Version] = noticeModule{path: module.Path, version: module.Version, dir: module.Dir}
	}
	if err := command.Wait(); err != nil {
		return fmt.Errorf("go list for %s: %w: %s", name, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func terminate(command *exec.Cmd) error {
	killErr := command.Process.Kill()
	if errors.Is(killErr, os.ErrProcessDone) {
		killErr = nil
	}
	waitErr := command.Wait()
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		waitErr = nil
	}
	return errors.Join(killErr, waitErr)
}

func findLicenseFiles(directory string) ([]string, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, entry := range entries {
		name := strings.ToLower(entry.Name())
		if entry.Type().IsRegular() && (strings.Contains(name, "license") || strings.HasPrefix(name, "copying") || strings.HasPrefix(name, "notice")) {
			files = append(files, entry.Name())
		}
	}
	slices.Sort(files)
	if len(files) == 0 {
		return nil, errors.New("no LICENSE, COPYING, or NOTICE file at module root")
	}
	return files, nil
}

func moduleDirectory(module noticeModule) string {
	value := strings.NewReplacer("/", "_", "\\", "_", "@", "_").Replace(module.path + "@" + module.version)
	return value
}

func copyFile(source, destination string) (retErr error) {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, input.Close()) }()
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fs.FileMode(0o644))
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, output.Close()) }()
	if _, err := io.Copy(output, input); err != nil {
		return err
	}
	return nil
}
