// conf/validate.go

package conf

import (
	"errors"
	"fmt"
	"log"
	"regexp"
)

// ValidationError represents a collection of validation errors
type ValidationError struct {
	Errors []string
}

// Error returns a string representation of the validation errors
func (ve ValidationError) Error() string {
	return fmt.Sprintf("Validation errors: %v", ve.Errors)
}

// ValidateSettings validates the entire Settings struct
func ValidateSettings(settings *Settings) error {
	ve := ValidationError{}

	// Validate BirdNET settings
	if err := validateBirdNETSettings(&settings.BirdNET); err != nil {
		ve.Errors = append(ve.Errors, err.Error())
	}

	// Validate OpenWeather settings
	if err := validateOpenWeatherSettings(&settings.Realtime.OpenWeather); err != nil {
		ve.Errors = append(ve.Errors, err.Error())
	}

	// Validate WebServer settings
	if err := validateWebServerSettings(&settings.WebServer); err != nil {
		ve.Errors = append(ve.Errors, err.Error())
	}

	// Validate Realtime settings
	if err := validateRealtimeSettings(&settings.Realtime); err != nil {
		ve.Errors = append(ve.Errors, err.Error())
	}

	// Validate Birdweather settings
	if err := validateBirdweatherSettings(&settings.Realtime.Birdweather); err != nil {
		ve.Errors = append(ve.Errors, err.Error())
	}

	// If there are any errors, return the ValidationError
	if len(ve.Errors) > 0 {
		return ve
	}
	return nil
}

// validateBirdNETSettings validates the BirdNET-specific settings
func validateBirdNETSettings(settings *struct {
	Sensitivity float64
	Threshold   float64
	Overlap     float64
	Longitude   float64
	Latitude    float64
	Threads     int
	Locale      string
	RangeFilter RangeFilterSettings
}) error {
	var errs []string

	// Check if sensitivity is within valid range
	if settings.Sensitivity < 0 || settings.Sensitivity > 1.5 {
		errs = append(errs, "BirdNET sensitivity must be between 0 and 1.5")
	}

	// Check if threshold is within valid range
	if settings.Threshold < 0 || settings.Threshold > 1 {
		errs = append(errs, "BirdNET threshold must be between 0 and 1")
	}

	// Check if overlap is within valid range
	if settings.Overlap < 0 || settings.Overlap > 2.9 {
		errs = append(errs, "BirdNET overlap value must be between 0 and 2.9 seconds")
	}

	// Check if longitude is within valid range
	if settings.Longitude < -180 || settings.Longitude > 180 {
		errs = append(errs, "BirdNET longitude must be between -180 and 180")
	}

	// Check if latitude is within valid range
	if settings.Latitude < -90 || settings.Latitude > 90 {
		errs = append(errs, "BirdNET latitude must be between -90 and 90")
	}

	// Check if threads is non-negative
	if settings.Threads < 0 {
		errs = append(errs, "BirdNET threads must be at least 0")
	}

	// Validate RangeFilter settings
	if settings.RangeFilter.Model == "" {
		errs = append(errs, "RangeFilter model must not be empty")
	}

	// Check if RangeFilter threshold is within valid range
	if settings.RangeFilter.Threshold < 0 || settings.RangeFilter.Threshold > 1 {
		errs = append(errs, "RangeFilter threshold must be between 0 and 1")
	}

	// If there are any errors, return them as a single error
	if len(errs) > 0 {
		return fmt.Errorf("BirdNET settings errors: %v", errs)
	}

	return nil
}

// validateOpenWeatherSettings validates the OpenWeather-specific settings
func validateOpenWeatherSettings(settings *OpenWeatherSettings) error {
	if settings.Enabled {
		// Check if API key is provided when enabled
		if settings.APIKey == "" {
			return errors.New("OpenWeather API key is required when enabled")
		}
		// Check if endpoint is provided when enabled
		if settings.Endpoint == "" {
			return errors.New("OpenWeather endpoint is required when enabled")
		}
		// Check if interval is at least 1 minute
		if settings.Interval < 1 {
			return errors.New("OpenWeather interval must be at least 1 minute")
		}
	}
	return nil
}

// validateWebServerSettings validates the WebServer-specific settings
func validateWebServerSettings(settings *struct {
	Enabled bool
	Port    string
	AutoTLS bool
	Log     LogConfig
}) error {
	if settings.Enabled {
		// Check if port is provided when enabled
		if settings.Port == "" {
			return errors.New("WebServer port is required when enabled")
		}
		// You might want to add more specific port validation here
	}
	return nil
}

// validateRealtimeSettings validates the Realtime-specific settings
func validateRealtimeSettings(settings *RealtimeSettings) error {
	// Check if interval is non-negative
	if settings.Interval < 0 {
		return errors.New("Realtime interval must be non-negative")
	}
	// Add more realtime settings validation as needed
	return nil
}

// validateBirdweatherSettings validates the Birdweather-specific settings
func validateBirdweatherSettings(settings *BirdweatherSettings) error {
	if settings.Enabled {
		// Check if ID is provided when enabled
		if settings.ID == "" {
			log.Println("Error: Birdweather ID is required when enabled. Disabling Birdweather.")
			settings.Enabled = false
			return nil
		}

		// Validate Birdweather ID format
		validIDPattern := regexp.MustCompile("^[a-zA-Z0-9]{24}$")
		if !validIDPattern.MatchString(settings.ID) {
			log.Println("Error: Invalid Birdweather ID format: must be 24 alphanumeric characters. Disabling Birdweather.")
			settings.Enabled = false
			return nil
		}

		// Check if threshold is within valid range
		if settings.Threshold < 0 || settings.Threshold > 1 {
			return errors.New("Birdweather threshold must be between 0 and 1")
		}

		// Check if location accuracy is non-negative
		if settings.LocationAccuracy < 0 {
			return errors.New("Birdweather location accuracy must be non-negative")
		}
	}
	return nil
}
