package main

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Row struct {
	Email          string
	UUID           string
	PhoneNumber    string
	TestIdentifier string
	EventName      string
	Timestamp      string
	TestParameter1 string
	TestParameter2 string
	EmailOptin     string
	TestAttr       string
	Age            string
	Country        string
	WhatsappOptin  string
	Birthday       string
	Currency       string
	Quantity       string
	EventGroupID   string
	UnitSalePrice  string
	UnitPrice      string
	UpdatedAt      string
}

var (
	eventNames = []string{
		"confirmation_page_view",
		"event_insider",
		"cart_cleared",
		"s3_event",
		"test_event",
	}
	countries = []string{
		"United States", "United Kingdom", "Germany", "France", "Japan",
		"Canada", "Australia", "Brazil", "India", "Mexico",
	}
	currencies = []string{"USD", "EUR", "GBP", "JPY", "CAD", "AUD", "BRL", "INR", "MXN"}
	words      = []string{
		"test", "data", "sample", "example", "demo", "trial", "mock", "fake",
	}
)

func generateRow(rng *rand.Rand) Row {
	return Row{
		Email:          fmt.Sprintf("user%d@example.com", rng.Int63()),
		UUID:           uuid.New().String(),
		PhoneNumber:    fmt.Sprintf("+1-555-%03d-%04d", rng.Intn(1000), rng.Intn(10000)),
		TestIdentifier: uuid.New().String(),
		EventName:      eventNames[rng.Intn(len(eventNames))],
		Timestamp:      time.Now().Add(-time.Duration(rng.Intn(365*24)) * time.Hour).Format(time.RFC3339),
		TestParameter1: fmt.Sprintf("VIN%017d", rng.Int63n(100000000000000000)),
		TestParameter2: fmt.Sprintf("LIC-%03d-%03d", rng.Intn(1000), rng.Intn(1000)),
		EmailOptin:     strconv.FormatBool(rng.Intn(2) == 1),
		TestAttr:       words[rng.Intn(len(words))],
		Age:            strconv.Itoa(rng.Intn(53) + 18),
		Country:        countries[rng.Intn(len(countries))],
		WhatsappOptin:  strconv.FormatBool(rng.Intn(2) == 1),
		Birthday:       time.Date(1950+rng.Intn(55), time.Month(rng.Intn(12)+1), rng.Intn(28)+1, 0, 0, 0, 0, time.UTC).Format("2006-01-02"),
		Currency:       currencies[rng.Intn(len(currencies))],
		Quantity:       strconv.Itoa(rng.Intn(1000) + 1),
		EventGroupID:   uuid.New().String(),
		UnitSalePrice:  fmt.Sprintf("%.2f", rng.Float64()*1000),
		UnitPrice:      fmt.Sprintf("%.2f", rng.Float64()*1000),
		UpdatedAt:      time.Now().Add(-time.Duration(rng.Intn(30*24)) * time.Hour).Format(time.RFC3339),
	}
}

func generateBatch(batchSize int, seed int64) []Row {
	rng := rand.New(rand.NewSource(seed))
	rows := make([]Row, batchSize)
	for i := 0; i < batchSize; i++ {
		rows[i] = generateRow(rng)
	}
	return rows
}

func rowToSlice(r Row) []string {
	return []string{
		r.Email, r.UUID, r.PhoneNumber, r.TestIdentifier, r.EventName,
		r.Timestamp, r.TestParameter1, r.TestParameter2, r.EmailOptin,
		r.TestAttr, r.Age, r.Country, r.WhatsappOptin, r.Birthday,
		r.Currency, r.Quantity, r.EventGroupID, r.UnitSalePrice,
		r.UnitPrice, r.UpdatedAt,
	}
}

func main() {
	rowCount := 50_000_000
	batchSize := 1_000_000
	numBatches := rowCount / batchSize
	numWorkers := 4 // Adjust based on CPU cores

	filename := fmt.Sprintf("%d_rows.csv", rowCount)
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	writer := csv.NewWriter(f)
	defer writer.Flush()

	// Write header
	header := []string{
		"email", "uuid", "phone_number", "test_identifier", "event_name",
		"timestamp", "test_parameter1", "test_parameter2", "email_optin",
		"test_attr", "age", "country", "whatsapp_optin", "birthday",
		"currency", "quantity", "event_group_id", "unit_sale_price",
		"unit_price", "updated_at",
	}
	writer.Write(header)

	// Use worker pool pattern for concurrent generation
	type batch struct {
		num  int
		rows []Row
	}

	batchChan := make(chan batch, numWorkers)
	var wg sync.WaitGroup

	// Start workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := workerID; i < numBatches; i += numWorkers {
				seed := time.Now().UnixNano() + int64(i)
				rows := generateBatch(batchSize, seed)
				batchChan <- batch{num: i, rows: rows}
			}
		}(w)
	}

	// Close channel when all workers done
	go func() {
		wg.Wait()
		close(batchChan)
	}()

	// Write batches as they come in
	processed := 0
	for b := range batchChan {
		for _, row := range b.rows {
			writer.Write(rowToSlice(row))
		}
		processed++
		fmt.Printf("Wrote batch %d/%d\n", processed, numBatches)
	}

	fmt.Println("Done writing rows")
}
