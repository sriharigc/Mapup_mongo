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
	}

	cursor, err := tollCollection.Aggregate(context.TODO(), pipeline)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Error executing aggregation pipeline: " + err.Error(),
		})
		return
	}
	defer cursor.Close(context.TODO())

	// Create a slice to store the result in the desired format
	var vehicleTollList []struct {
		VehicleID string  `json:"vehicle_id"`
		TollCost  float64 `json:"toll_cost"`
	}

	for cursor.Next(context.TODO()) {
		var result struct {
			VehicleID     string  `bson:"_id"`
			TotalTollCost float64 `bson:"totalTollCost"`
		}
		if err := cursor.Decode(&result); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Error decoding aggregation result: " + err.Error(),
			})
			return
		}
		// Append each result as an object with "vehicle_id" and "toll_cost"
		vehicleTollList = append(vehicleTollList, struct {
			VehicleID string  `json:"vehicle_id"`
			TollCost  float64 `json:"toll_cost"`
		}{
			VehicleID: result.VehicleID,
			TollCost:  result.TotalTollCost,
		})
	}

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

	// Fetch trips for the user within the date range, using parsed times instead of strings
	var trips []TripDetail
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

	var tripsWithTolls []TripWithTolls

	// Iterate through trips and fetch tolls for each trip
	for tripCursor.Next(context.TODO()) {
		var trip TripDetail
		if err := tripCursor.Decode(&trip); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decode trip"})
			return
		}

		// Append the trip to the trips slice
		trips = append(trips, trip)

		// Fetch tolls corresponding to the trip's start_time and end_time
		var tolls []TollData
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

		// Decode each toll record and append to the tolls slice
		for tollCursor.Next(context.TODO()) {
			var toll TollData
			if err := tollCursor.Decode(&toll); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decode toll"})
				return
			}
			tolls = append(tolls, toll)
		}

		// Create a TripWithTolls object for the trip and corresponding tolls
		tripsWithTolls = append(tripsWithTolls, TripWithTolls{
			Trip:  trip,
			Tolls: tolls,
		})
	}

	if err := tripCursor.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Cursor error while fetching trips"})
		return
	}

	// You can use `trips` here if you need to do something with the trip data

	// Return trips with tolls in the response
	c.JSON(http.StatusOK, gin.H{"trips_with_tolls": tripsWithTolls})
}

func main() {
	initMongo()

	router := gin.Default()

	router.GET("/trips_with_tolls", getTotalTollCostForDateRange)

	router.GET("/tolls", FetchTollsForUser)
	// Start the server
	router.Run(":8080")
}
