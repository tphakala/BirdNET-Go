// processor/actions.go

package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tphakala/birdnet-go/internal/birdnet"
	"github.com/tphakala/birdnet-go/internal/birdweather"
	"github.com/tphakala/birdnet-go/internal/conf"
	"github.com/tphakala/birdnet-go/internal/datastore"
	"github.com/tphakala/birdnet-go/internal/imageprovider"
	"github.com/tphakala/birdnet-go/internal/mqtt"
	"github.com/tphakala/birdnet-go/internal/myaudio"
	"github.com/tphakala/birdnet-go/internal/observation"
)

type Action interface {
	Execute(data interface{}) error
}

type LogAction struct {
	Settings     *conf.Settings
	Note         datastore.Note
	EventTracker *EventTracker
	mu           sync.Mutex // Protect concurrent access to Note
}

type DatabaseAction struct {
	Settings     *conf.Settings
	Ds           datastore.Interface
	Note         datastore.Note
	Results      []datastore.Results
	EventTracker *EventTracker
	mu           sync.Mutex // Protect concurrent access to Note and Results
}

type SaveAudioAction struct {
	Settings     *conf.Settings
	ClipName     string
	pcmData      []byte
	EventTracker *EventTracker
	mu           sync.Mutex // Protect concurrent access to pcmData
}

type BirdWeatherAction struct {
	Settings     *conf.Settings
	Note         datastore.Note
	pcmData      []byte
	BwClient     *birdweather.BwClient
	EventTracker *EventTracker
	mu           sync.Mutex // Protect concurrent access to Note and pcmData
}

type MqttAction struct {
	Settings       *conf.Settings
	Note           datastore.Note
	BirdImageCache *imageprovider.BirdImageCache
	MqttClient     mqtt.Client
	EventTracker   *EventTracker
	mu             sync.Mutex // Protect concurrent access to Note
}

type UpdateRangeFilterAction struct {
	Bn       *birdnet.BirdNET
	Settings *conf.Settings
	mu       sync.Mutex // Protect concurrent access to Settings
}

// Execute logs the note to the chag log file
func (a *LogAction) Execute(data interface{}) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	species := strings.ToLower(a.Note.CommonName)

	// Check if the event should be handled for this species
	if !a.EventTracker.TrackEvent(species, LogToFile) {
		return nil
	}

	// Log note to file
	if err := observation.LogNoteToFile(a.Settings, &a.Note); err != nil {
		// If an error occurs when logging to a file, wrap and return the error.
		log.Printf("Failed to log note to file: %v", err)
	}
	fmt.Printf("%s %s %.2f\n", a.Note.Time, a.Note.CommonName, a.Note.Confidence)

	return nil
}

// Execute saves the note to the database
func (a *DatabaseAction) Execute(data interface{}) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	species := strings.ToLower(a.Note.CommonName)

	// Check event frequency
	if !a.EventTracker.TrackEvent(species, DatabaseSave) {
		return nil
	}

	// Save note to database
	if err := a.Ds.Save(&a.Note, a.Results); err != nil {
		log.Printf("Failed to save note and results to database: %v", err)
		return err
	}

	// Save audio clip to file if enabled
	if a.Settings.Realtime.Audio.Export.Enabled {
		// export audio clip from capture buffer
		pcmData, err := myaudio.ReadSegmentFromCaptureBuffer(a.Note.Source, a.Note.BeginTime, 15)
		if err != nil {
			log.Printf("Failed to read audio segment from buffer: %v", err)
			return err
		}

		// Create a SaveAudioAction and execute it
		saveAudioAction := &SaveAudioAction{
			Settings: a.Settings,
			ClipName: a.Note.ClipName,
			pcmData:  pcmData,
		}

		if err := saveAudioAction.Execute(nil); err != nil {
			log.Printf("Failed to save audio clip: %v", err)
			return err
		}

		if a.Settings.Debug {
			log.Printf("Saved audio clip to %s\n", a.Note.ClipName)
			log.Printf("detection time %v, begin time %v, end time %v\n", a.Note.Time, a.Note.BeginTime, time.Now())
		}
	}

	return nil
}

// Execute saves the audio clip to a file
func (a *SaveAudioAction) Execute(data interface{}) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Get the full path by joining the export path with the relative clip name
	outputPath := filepath.Join(a.Settings.Realtime.Audio.Export.Path, a.ClipName)

	// Ensure the directory exists
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		log.Printf("error creating directory for audio clip: %s\n", err)
		return err
	}

	if a.Settings.Realtime.Audio.Export.Type == "wav" {
		if err := myaudio.SavePCMDataToWAV(outputPath, a.pcmData); err != nil {
			log.Printf("error saving audio clip to WAV: %s\n", err)
			return err
		}
	} else {
		if err := myaudio.ExportAudioWithFFmpeg(a.pcmData, outputPath, &a.Settings.Realtime.Audio); err != nil {
			log.Printf("error exporting audio clip with FFmpeg: %s\n", err)
			return err
		}
	}

	if a.Settings.Debug {
		log.Printf("Saved audio clip to %s\n", outputPath)
	}
	return nil
}

/*func (a SaveAudioAction) Execute(data interface{}) error {
	if err := myaudio.SavePCMDataToWAV(a.ClipName, a.pcmData); err != nil {
		log.Printf("error saving audio clip to %s: %s\n", a.Settings.Realtime.Audio.Export.Type, err)
		return err
	} else if a.Settings.Debug {
		log.Printf("Saved audio clip to %s\n", a.ClipName)
	}
	return nil // return an error if the action fails
}*/

// Execute sends the note to the BirdWeather API
func (a *BirdWeatherAction) Execute(data interface{}) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	species := strings.ToLower(a.Note.CommonName)

	// Check event frequency
	if !a.EventTracker.TrackEvent(species, BirdWeatherSubmit) {
		return nil
	}

	// Add threshold check here
	if a.Note.Confidence < float64(a.Settings.Realtime.Birdweather.Threshold) {
		if a.Settings.Debug {
			log.Printf("Skipping BirdWeather upload for %s: confidence %.2f below threshold %.2f\n",
				species, a.Note.Confidence, a.Settings.Realtime.Birdweather.Threshold)
		}
		return nil
	}

	if a.BwClient == nil {
		return fmt.Errorf("BirdWeather client is not initialized")
	}

	if err := a.BwClient.Publish(a.Note, a.pcmData); err != nil {
		log.Printf("error uploading to BirdWeather: %s\n", err)
		return err
	} else if a.Settings.Debug {
		log.Printf("Uploaded %s to Birdweather\n", a.Note.ClipName)
	}
	return nil
}

type NoteWithBirdImage struct {
	datastore.Note
	BirdImage imageprovider.BirdImage
}

// Execute sends the note to the MQTT broker
func (a *MqttAction) Execute(data interface{}) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	species := strings.ToLower(a.Note.CommonName)

	// Check event frequency
	if !a.EventTracker.TrackEvent(species, MQTTPublish) {
		return nil
	}

	// First, check if the MQTT client is connected
	if !a.MqttClient.IsConnected() {
		log.Println("MQTT client is not connected, skipping publish")
		return nil
	}

	// Validate MQTT settings
	if a.Settings.Realtime.MQTT.Topic == "" {
		return fmt.Errorf("MQTT topic is not specified")
	}

	// Get bird image of detected bird
	birdImage, err := a.BirdImageCache.Get(a.Note.ScientificName)
	if err != nil {
		birdImage = imageprovider.BirdImage{}
	}

	// Wrap note with bird image
	noteWithBirdImage := NoteWithBirdImage{Note: a.Note, BirdImage: birdImage}

	// Create a JSON representation of the note
	noteJson, err := json.Marshal(noteWithBirdImage)
	if err != nil {
		log.Printf("error marshalling note to JSON: %s\n", err)
		return err
	}

	// Create a context with timeout for publishing
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish the note to the MQTT broker
	err = a.MqttClient.Publish(ctx, a.Settings.Realtime.MQTT.Topic, string(noteJson))
	if err != nil {
		return fmt.Errorf("failed to publish to MQTT: %w", err)
	}

	return nil
}

// Execute updates the range filter species list, this is run every day
func (a *UpdateRangeFilterAction) Execute(data interface{}) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	today := time.Now().Truncate(24 * time.Hour)
	if today.After(a.Settings.BirdNET.RangeFilter.LastUpdated) {
		// Update location based species list
		speciesScores, err := a.Bn.GetProbableSpecies(today, 0.0)
		if err != nil {
			return err
		}

		// Convert the speciesScores slice to a slice of species labels
		var includedSpecies []string
		for _, speciesScore := range speciesScores {
			includedSpecies = append(includedSpecies, speciesScore.Label)
		}

		a.Settings.UpdateIncludedSpecies(includedSpecies)
	}
	return nil
}
