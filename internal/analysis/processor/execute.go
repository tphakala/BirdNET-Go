// execute.go
package processor

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"

	"github.com/tphakala/birdnet-go/internal/datastore"
)

type ExecuteCommandAction struct {
	Command string
	Params  map[string]interface{}
}

func (a ExecuteCommandAction) Execute(data interface{}) error {
	log.Printf("[analysis/processor/execute] Executing command: %s params: %v\n", a.Command, a.Params)

	// Type assertion to check if data is of type Detections
	detection, ok := data.(Detections)
	if !ok {
		return fmt.Errorf("ExecuteScriptAction requires Detections type, got %T", data)
	}

	// Validate and resolve the command path
	cmdPath, err := validateCommandPath(a.Command)
	if err != nil {
		return fmt.Errorf("invalid command path: %w", err)
	}

	// Building the command line arguments with validation
	args, err := buildSafeArguments(a.Params, &detection.Note)
	if err != nil {
		return fmt.Errorf("error building arguments: %w", err)
	}

	log.Printf("[analysis/processor/execute] Command: %s, Args: %v\n", cmdPath, args)

	// Create command with validated path and arguments
	cmd := exec.Command(cmdPath, args...)

	// Set a clean environment
	cmd.Env = getCleanEnvironment()

	// Execute the command
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error executing command: %w, output: %s", err, string(output))
	}

	log.Printf("[analysis/processor/execute] Command executed successfully: %s", string(output))
	return nil
}

// validateCommandPath ensures the command exists and is executable
func validateCommandPath(command string) (string, error) {
	// Clean the path to remove any potential directory traversal
	command = filepath.Clean(command)

	// Check if it's an absolute path
	if !filepath.IsAbs(command) {
		return "", fmt.Errorf("command must use absolute path: %s", command)
	}

	// Verify the file exists and is executable
	info, err := os.Stat(command)
	if err != nil {
		return "", fmt.Errorf("command not found: %w", err)
	}

	// Check file permissions
	if runtime.GOOS != "windows" {
		if info.Mode()&0o111 == 0 {
			return "", fmt.Errorf("command is not executable: %s", command)
		}
	}

	return command, nil
}

// buildSafeArguments creates a sanitized list of command arguments
func buildSafeArguments(params map[string]interface{}, note *datastore.Note) ([]string, error) {
	var args []string

	for key, value := range params {
		// Validate parameter name (allow only alphanumeric and _-)
		if !isValidParamName(key) {
			return nil, fmt.Errorf("invalid parameter name: %s", key)
		}

		// Get value from Note or use default
		noteValue := getNoteValueByName(note, key)
		if noteValue == nil {
			noteValue = value
		}

		// Convert and validate the value
		strValue, err := sanitizeValue(noteValue)
		if err != nil {
			return nil, fmt.Errorf("invalid value for parameter %s: %w", key, err)
		}

		// Quote the value if it contains spaces or special characters
		if strings.ContainsAny(strValue, " \t\n\r\"'") {
			// Escape any existing quotes in the value
			strValue = strings.ReplaceAll(strValue, `"`, `\"`)
			// Wrap the value in quotes
			strValue = fmt.Sprintf(`"%q"`, strValue)
		}

		arg := fmt.Sprintf("--%s=%s", key, strValue)
		args = append(args, arg)
	}

	return args, nil
}

// isValidParamName checks if a parameter name contains only safe characters
func isValidParamName(name string) bool {
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' || r == '-') {
			return false
		}
	}
	return true
}

// sanitizeValue converts and validates a value to string
func sanitizeValue(value interface{}) (string, error) {
	// Convert to string and validate
	str := fmt.Sprintf("%v", value)

	// Basic sanitization - remove any control characters
	str = strings.Map(func(r rune) rune {
		if r < 32 {
			return -1
		}
		return r
	}, str)

	// Additional validation can be added here

	return str, nil
}

// getCleanEnvironment returns a minimal set of environment variables necessary for safe command execution across different operating systems. It includes essential system paths and optionally adds the SystemRoot variable for Windows systems. The returned slice contains only the required environment variables, helping to maintain a clean and secure execution context.
func getCleanEnvironment() []string {
	// Provide only necessary environment variables
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"TEMP=" + os.Getenv("TEMP"),
		"TMP=" + os.Getenv("TMP"),
	}

	// Add system root for Windows
	if runtime.GOOS == "windows" {
		env = append(env, "SystemRoot="+os.Getenv("SystemRoot"))
	}

	return env
}

// If the field is not found or cannot be interfaced, it returns nil.
func getNoteValueByName(note *datastore.Note, paramName string) interface{} {
	val := reflect.ValueOf(*note)
	fieldVal := val.FieldByName(paramName)

	// Check if the field is valid (exists in the struct) and can be interfaced
	if fieldVal.IsValid() && fieldVal.CanInterface() {
		return fieldVal.Interface()
	}

	// Return nil or an appropriate zero value if the field does not exist
	return nil
}
