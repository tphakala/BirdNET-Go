// utils.go: This file contains utility functions for the HTTP controller package.
package httpcontroller

import (
	"fmt"
	"html"
	"html/template"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/tphakala/birdnet-go/internal/datastore"
)

// NoteWithIndex extends model.Note with additional fields for template rendering.
type NoteWithIndex struct {
	datastore.Note
	HourlyCounts    [24]int // Hourly occurrence counts of the note
	TotalDetections int     // Total number of detections for the note
	Index           int     // Index in a list for rendering purposes
}

// getCurrentDate returns the current date in YYYY-MM-DD format.
func getCurrentDate() string {
	return time.Now().Format("2006-01-02")
}

// calcWidth calculates the width of a bar in a bar chart as a percentage.
// It normalizes the totalDetections based on a predefined maximum.
func calcWidth(totalDetections int) int {
	const maxDetections = 200 // Maximum number of detections expected
	widthPercentage := (totalDetections * 100) / maxDetections
	if widthPercentage > 100 {
		widthPercentage = 100 // Limit width to 100% if exceeded
	}
	return widthPercentage
}

// even checks if an integer is even. Useful for alternating styles in loops.
func even(index int) bool {
	return index%2 == 0
}

// heatmapColor assigns a color based on a provided value using predefined thresholds.
func heatmapColor(value int) string {
	thresholds := []int{10, 20, 30, 40, 50, 60, 70, 80, 90}
	colors := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}

	for i, threshold := range thresholds {
		if value <= threshold {
			return colors[i]
		}
	}
	return colors[len(colors)-1] // Default to the highest color for values above all thresholds
}

// confidence converts a confidence value (0.0 - 1.0) to a percentage string.
func confidence(confidence float64) string {
	return fmt.Sprintf("%.0f%%", confidence*100)
}

// confidenceColor assigns a color based on the confidence value.
func confidenceColor(confidence float64) string {
	switch {
	case confidence >= 0.8:
		return "bg-green-500" // High confidence
	case confidence >= 0.4:
		return "bg-orange-400" // Moderate confidence
	default:
		return "bg-red-500" // Low confidence
	}
}

// createSpectrogramWithSoX generates a spectrogram for a WAV file using SoX.
func createSpectrogramWithSoX(audioClipPath, spectrogramPath string, width int) error {
	// Verify SoX installation
	if _, err := exec.LookPath("sox"); err != nil {
		return fmt.Errorf("SoX binary not found: %w", err)
	}

	// Set height based on width
	heightStr := strconv.Itoa(width / 2)
	widthStr := strconv.Itoa(width)

	// Build SoX command arguments
	args := []string{audioClipPath, "-n", "rate", "24k", "spectrogram", "-x", widthStr, "-y", heightStr, "-o", spectrogramPath}
	if width < 800 {
		args = append(args, "-r")
	}

	// Determine the command based on the OS
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		// Directly use SoX command on Windows
		cmd = exec.Command("sox", args...)
	} else {
		// Prepend 'nice' to the command on Unix-like systems
		args = append([]string{"-n", "10", "sox"}, args...) // '19' is a nice value for low priority
		cmd = exec.Command("nice", args...)
	}

	// Execute the command
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("SoX command failed: %w", err)
	}

	log.Printf("Spectrogram generated at '%s'", spectrogramPath)
	return nil
}

// GetSpectrogramPath returns the web-friendly path to the spectrogram image for a WAV file, stored in the same directory.
func (s *Server) getSpectrogramPath(wavFileName string, width int) (string, error) {
	baseName := filepath.Base(wavFileName)
	dir := filepath.Dir(wavFileName)
	ext := filepath.Ext(baseName)
	baseNameWithoutExt := baseName[:len(baseName)-len(ext)]

	// Include width in the filename
	spectrogramFileName := fmt.Sprintf("%s_%dpx.png", baseNameWithoutExt, width)

	// Construct the file system path using filepath.Join to ensure it's valid on the current OS.
	spectrogramPath := filepath.Join(dir, spectrogramFileName)

	// Convert the file system path to a web-friendly path by replacing backslashes with forward slashes.
	webFriendlyPath := strings.Replace(spectrogramPath, "\\", "/", -1)

	// Check if spectrogram already exists
	if _, err := os.Stat(spectrogramPath); os.IsNotExist(err) {
		// Create the spectrogram if it doesn't exist
		if err := createSpectrogramWithSoX(wavFileName, spectrogramPath, width); err != nil {
			return "", fmt.Errorf("error creating spectrogram with SoX: %w", err)
		}
	} else if err != nil {
		return "", fmt.Errorf("error checking spectrogram file: %w", err)
	}

	// Return the web-friendly path
	return webFriendlyPath, nil
}

// sumHourlyCounts calculates the total counts from hourly counts.
func sumHourlyCounts(hourlyCounts [24]int) int {
	total := 0
	for _, count := range hourlyCounts {
		total += count
	}
	return total
}

// makeHoursSlice creates a slice representing 24 hours.
func makeHoursSlice() []int {
	hours := make([]int, 24)
	for i := range hours {
		hours[i] = i
	}
	return hours
}

// parseNumDetections parses a string to an integer or returns a default value.
func parseNumDetections(numDetectionsStr string, defaultValue int) int {
	if numDetectionsStr == "" {
		return defaultValue
	}
	numDetections, err := strconv.Atoi(numDetectionsStr)
	if err != nil || numDetections <= 0 {
		return defaultValue
	}
	return numDetections
}

// parseOffset converts the offset query parameter to an integer.
func parseOffset(offsetStr string, defaultOffset int) int {
	if offsetStr == "" {
		return defaultOffset
	}
	offset, err := strconv.Atoi(offsetStr)
	if err != nil {
		return defaultOffset
	}
	return offset
}

// Thumbnail returns the URL of a given bird's thumbnail image.
// It takes the bird's scientific name as input and returns the image URL as a string.
// If the image is not found or an error occurs, it returns an empty string.
func (s *Server) thumbnail(scientificName string) string {
	if s.BirdImageCache == nil {
		// Return empty string if the cache is not initialized
		return ""
	}

	birdImage, err := s.BirdImageCache.Get(scientificName)
	if err != nil {
		// Return empty string if an error occurs
		return ""
	}

	return birdImage.URL
}

// ThumbnailAttribution returns the HTML-formatted attribution for a bird's thumbnail image.
// It takes the bird's scientific name as input and returns a template.HTML string.
// If the attribution information is incomplete or an error occurs, it returns an empty template.HTML.
func (s *Server) thumbnailAttribution(scientificName string) template.HTML {
	if s.BirdImageCache == nil {
		// Return empty string if the cache is not initialized
		return template.HTML("")
	}

	birdImage, err := s.BirdImageCache.Get(scientificName)
	if err != nil {
		log.Printf("Error getting thumbnail info for %s: %v", scientificName, err)
		return template.HTML("")
	}

	if birdImage.AuthorName == "" || birdImage.LicenseName == "" {
		return template.HTML("")
	}

	var attribution string
	if birdImage.AuthorURL == "" {
		attribution = fmt.Sprintf("© %s / <a href=\"%s\">%s</a>",
			html.EscapeString(birdImage.AuthorName),
			html.EscapeString(birdImage.LicenseURL),
			html.EscapeString(birdImage.LicenseName))
	} else {
		attribution = fmt.Sprintf("© <a href=\"%s\">%s</a> / <a href=\"%s\">%s</a>",
			html.EscapeString(birdImage.AuthorURL),
			html.EscapeString(birdImage.AuthorName),
			html.EscapeString(birdImage.LicenseURL),
			html.EscapeString(birdImage.LicenseName))
	}

	return template.HTML(attribution)
}
