package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type TripDetail struct {
	ID          primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	UserID      string             `bson:"user_id" json:"user_id"`
	Marketplace string             `bson:"marketplace" json:"marketplace"`
	StartTime   string             `bson:"start_time" json:"start_time"`
	EndTime     string             `bson:"end_time" json:"end_time"`
	VehicleID   string             `bson:"vehicle_id" json:"vehicle_id"`
}

type TollData struct {
	ID               primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	GeoFenceIDStart  int                `bson:"geoFence_id_start" json:"geoFence_id_start"`
	GeoFenceIDEnd    int                `bson:"geoFence_id_end,omitempty" json:"geoFence_id_end,omitempty"`
	NameStart        string             `bson:"name_start" json:"name_start"`
	RoadStart        string             `bson:"road_start" json:"road_start"`
	NameEnd          string             `bson:"name_end,omitempty" json:"name_end,omitempty"`
	RoadEnd          string             `bson:"road_end,omitempty" json:"road_end,omitempty"`
	TollSystemType   string             `bson:"toll_system_type,omitempty" json:"toll_system_type,omitempty"`
	EntryLat         float64            `bson:"entry_lat" json:"entry_lat"`
	EntryLng         float64            `bson:"entry_lng" json:"entry_lng"`
	ExitLat          float64            `bson:"exit_lat,omitempty" json:"exit_lat,omitempty"`
	ExitLng          float64            `bson:"exit_lng,omitempty" json:"exit_lng,omitempty"`
	TagCost          float64            `bson:"tag_cost,omitempty" json:"tag_cost,omitempty"`
	CashCost         float64            `bson:"cash_cost,omitempty" json:"cash_cost,omitempty"`
	LisencePlateCost float64            `bson:"lisence_plate_cost,omitempty" json:"lisence_plate_cost,omitempty"`
	TagAndCashCost   float64            `bson:"tag_and_cash_cost,omitempty" json:"tag_and_cash_cost,omitempty"`
	EntryTime        string             `bson:"entry_time" json:"entry_time"`
	ExitTime         string             `bson:"exit_time,omitempty" json:"exit_time,omitempty"`
	ExpressLaneCost  float64            `bson:"express_lane_cost,omitempty" json:"express_lane_cost,omitempty"`
	IsExpressLane    bool               `bson:"is_expresslane,omitempty" json:"is_expresslane,omitempty"`
	Currency         string             `bson:"currency,omitempty" json:"currency,omitempty"`
	TollAgencyName   []string           `bson:"toll_agency_name,omitempty" json:"toll_agency_name,omitempty"`
	TollAgencyAbbr   []string           `bson:"toll_agency_abbr,omitempty" json:"toll_agency_abbr,omitempty"`
	JobID            string             `bson:"job_id" json:"job_id"`
}
type VehicleDetail struct {
	VehicleID string `bson:"vehicle_id" json:"vehicle_id"`
	VIN       string `bson:"vin" json:"vin"`
	Name      string `bson:"name" json:"name"`
	Model     string `bson:"model" json:"model"`
	Make      string `bson:"make" json:"make"`
}
type TripWithTolls struct {
	Trip  TripDetail `json:"trip"`
	Tolls []TollData `json:"tolls"`
}

var tripCollection *mongo.Collection
var tollCollection *mongo.Collection

func initMongo() {
	// Replace with your connection string
	uri := "mongodb+srv://sriharigc:Srih%40ri03.@cluster0.4rxyv.mongodb.net/"

	clientOptions := options.Client().ApplyURI(uri)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to MongoDB!")

	tripCollection = client.Database("Mapup").Collection("Mongo collection1")
	tollCollection = client.Database("Mapup").Collection("Mongo collection 2")
}

// Function to validate the time format
func isValidRFC3339Time(timeStr string) bool {
	_, err := time.Parse(time.RFC3339, timeStr)
	return err == nil
}

func getTotalTollCostForDateRange(c *gin.Context) {
	userID := c.Query("user_id")
	startTimeStr := c.Query("start_time")
	endTimeStr := c.Query("end_time")

	if userID == "" || startTimeStr == "" || endTimeStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Missing required query parameters: user_id, start_time, end_time",
		})
		return
	}

	// Validate time format
	if !isValidRFC3339Time(startTimeStr) || !isValidRFC3339Time(endTimeStr) {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid time format. Use RFC3339 format (e.g., 2024-04-09T06:35:33Z)",
		})
		return
	}

	// Parse start_time and end_time
	startTime, err := time.Parse(time.RFC3339, startTimeStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid start_time format. Use RFC3339 format (e.g., 2024-04-09T06:35:33Z)",
		})
		return
	}

	endTime, err := time.Parse(time.RFC3339, endTimeStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid end_time format. Use RFC3339 format (e.g., 2024-04-09T07:16:09Z)",
		})
		return
	}

	// MongoDB aggregation pipeline
	pipeline := mongo.Pipeline{
		// Convert string dates to actual Date objects
		{{Key: "$addFields", Value: bson.D{
			{Key: "entry_time", Value: bson.D{{Key: "$dateFromString", Value: bson.D{
				{Key: "dateString", Value: "$entry_time"},
			}}}},
		}}},

		// Match documents based on user ID and date range
		{{Key: "$match", Value: bson.D{
			{Key: "user_id", Value: userID},
			{Key: "entry_time", Value: bson.D{{Key: "$gte", Value: startTime}}},
			{Key: "entry_time", Value: bson.D{{Key: "$lte", Value: endTime}}},
		}}},

		// Group by vehicle_id and sum the total toll cost
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$vehicle_id"},
			{Key: "totalTollCost", Value: bson.D{{Key: "$sum", Value: "$tag_and_cash_cost"}}},
		}}},

		// Lookup vehicle details
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "Mongo collection 3"},
			{Key: "localField", Value: "_id"},
			{Key: "foreignField", Value: "vehicle_id"},
			{Key: "as", Value: "vehicle_details"},
		}}},

		// Optionally, unwind the vehicle details array
		{{Key: "$unwind", Value: bson.D{
			{Key: "path", Value: "$vehicle_details"},
			{Key: "preserveNullAndEmptyArrays", Value: true}, // Optional: keep results without vehicle details
		}}},
	}

	cursor, err := tollCollection.Aggregate(context.TODO(), pipeline)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Error executing aggregation pipeline: " + err.Error(),
		})
		return
	}
	defer cursor.Close(context.TODO())

	// Define a new struct for the flattened response
	type VehicleToll struct {
		VehicleID string  `json:"vehicle_id"`
		TollCost  float64 `json:"toll_cost"`
		VIN       string  `json:"vin"`
		Name      string  `json:"name"`
		Model     string  `json:"model"`
		Make      string  `json:"make"`
	}

	var vehicleTollList []VehicleToll

	for cursor.Next(context.TODO()) {
		var result struct {
			VehicleID     string        `bson:"_id"`
			TotalTollCost float64       `bson:"totalTollCost"`
			VehicleDetail VehicleDetail `bson:"vehicle_details"`
		}
		if err := cursor.Decode(&result); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Error decoding aggregation result: " + err.Error(),
			})
			return
		}

		// Ensure that VehicleDetail is not nil
		if (VehicleDetail{}) == result.VehicleDetail {
			// Handle cases where vehicle_details are missing if necessary
			// For now, we'll leave the fields empty
		}

		// Append each result as a flattened object with "vehicle_id", "toll_cost", and vehicle details
		vehicleTollList = append(vehicleTollList, VehicleToll{
			VehicleID: result.VehicleID,
			TollCost:  result.TotalTollCost,
			VIN:       result.VehicleDetail.VIN,
			Name:      result.VehicleDetail.Name,
			Model:     result.VehicleDetail.Model,
			Make:      result.VehicleDetail.Make,
		})
	}

	if err := cursor.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Cursor error while fetching toll data"})
		return
	}

	// Return the flattened data in the response
	c.JSON(http.StatusOK, gin.H{
		"data": vehicleTollList,
	})
}

// FetchTollsForUser handles the endpoint to fetch toll details for a user within a date range
func FetchTollsForUser(c *gin.Context) {
	userID := c.Query("user_id")
	startTimeStr := c.Query("start_time")
	endTimeStr := c.Query("end_time")

	if userID == "" || startTimeStr == "" || endTimeStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Missing required query parameters: user_id, start_time, end_time",
		})
		return
	}

	// Parse start_time and end_time
	startTime, err := time.Parse("2006-01-02T15:04:05Z", startTimeStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid start_time format. Use RFC3339 format (e.g., 2024-04-09T06:35:33Z)",
		})
		return
	}

	endTime, err := time.Parse("2006-01-02T15:04:05Z", endTimeStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid end_time format. Use RFC3339 format (e.g., 2024-04-09T06:35:33Z)",
		})
		return
	}

	// Fetch trips for the user within the date range, using parsed times
	tripCursor, err := tripCollection.Find(context.TODO(), bson.M{
		"user_id": userID,
		"start_time": bson.M{
			"$gte": startTime.Format(time.RFC3339), // Format the parsed time back to RFC3339 string for MongoDB
		},
		"end_time": bson.M{
			"$lte": endTime.Format(time.RFC3339), // Format the parsed time back to RFC3339 string for MongoDB
		},
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch trips"})
		return
	}
	defer tripCursor.Close(context.TODO())

	// Result slice to store the flattened structure
	var tripsWithTolls []gin.H

	// Iterate through trips and fetch tolls for each trip
	for tripCursor.Next(context.TODO()) {
		var trip TripDetail
		if err := tripCursor.Decode(&trip); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decode trip"})
			return
		}

		// Fetch tolls corresponding to the trip's start_time and end_time
		var tolls []gin.H
		tollCursor, err := tollCollection.Find(context.TODO(), bson.M{
			"user_id": userID,
			"entry_time": bson.M{
				"$gte": trip.StartTime, // Use the trip's start_time
				"$lte": trip.EndTime,   // Use the trip's end_time
			},
		})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch tolls for trip"})
			return
		}
		defer tollCursor.Close(context.TODO())

		// Decode each toll record and add it to the tolls slice
		for tollCursor.Next(context.TODO()) {
			var toll TollData
			if err := tollCursor.Decode(&toll); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decode toll"})
				return
			}
			tolls = append(tolls, gin.H{
				"id":                toll.ID,
				"geoFence_id_start": toll.GeoFenceIDStart,
				"name_start":        toll.NameStart,
				"road_start":        toll.RoadStart,
				"toll_system_type":  toll.TollSystemType,
				"entry_lat":         toll.EntryLat,
				"entry_lng":         toll.EntryLng,
				"tag_cost":          toll.TagCost,
				"tag_and_cash_cost": toll.TagAndCashCost,
				"entry_time":        toll.EntryTime,
				"toll_agency_name":  toll.TollAgencyName,
				"toll_agency_abbr":  toll.TollAgencyAbbr,
				"job_id":            toll.JobID,
			})
		}

		// Append the trip details along with the tolls as a flattened structure
		tripsWithTolls = append(tripsWithTolls, gin.H{
			"id":          trip.ID,
			"user_id":     trip.UserID,
			"marketplace": trip.Marketplace,
			"start_time":  trip.StartTime,
			"end_time":    trip.EndTime,
			"vehicle_id":  trip.VehicleID,
			"tolls":       tolls,
		})
	}

	if err := tripCursor.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Cursor error while fetching trips"})
		return
	}

	// Return the flattened result in the response
	c.JSON(http.StatusOK, gin.H{"data": tripsWithTolls})
}

func main() {
	initMongo()

	router := gin.Default()

	router.GET("/trips_with_tolls", getTotalTollCostForDateRange)

	router.GET("/tolls", FetchTollsForUser)
	// Start the server
	router.Run(":8080")
}
