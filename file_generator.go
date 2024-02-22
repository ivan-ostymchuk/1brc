package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"
)

var mutexFile = &sync.RWMutex{}

type Station struct {
	id              string
	meanTemperature float64
}

func NewStation(id string, meanTemperature float64) *Station {
	// pseudorandom normally distributed number with normal distribution mean (meanTemperature) and standard deviation (10)
	return &Station{id: id, meanTemperature: meanTemperature}
}

func (s *Station) temperature(randomGenerator *rand.Rand) float64 {
	randFloat := randomGenerator.NormFloat64()*10 + s.meanTemperature
	roundedFloat := roundFloat(randFloat, 1)
	return roundedFloat
}

func generateMeasurementFile(numberOfRows int) error {

	filename := "measurements.txt"
	stations := getListOfStations()

	// Create the output file
	os.Create(filename)
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Calculate the number of rows to be written by each worker
	maxGoRoutines := runtime.GOMAXPROCS(0)
	rowsPerTask := numberOfRows / maxGoRoutines

	var wg sync.WaitGroup
	errCh := make(chan error, maxGoRoutines)

	for i := 0; i < maxGoRoutines; i++ {
		wg.Add(1)
		go generateData(file, &wg, rowsPerTask, stations, errCh)
	}

	// Close the error channel when all workers are done
	go func() {
		wg.Wait()
		close(errCh)
	}()

	// Collect and handle errors
	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

func generateData(
	file *os.File,
	wg *sync.WaitGroup,
	rowsPerTask int,
	stations []*Station,
	errCh chan error,
) {
	defer wg.Done()
	// each goroutine should have its own instance of random generator because rand.Rand is not safe concurrently. It throws an out of index error.
	randomGenerator := rand.New(rand.NewSource(time.Now().UnixNano()))
	startTime := time.Now()
	bufSize := 65536
	writer := bufio.NewWriterSize(file, bufSize)

	for i := 0; i < rowsPerTask; i++ {
		if i > 0 && i%50000000 == 0 {
			fmt.Printf("Wrote %d measurements in %d \n", i, time.Since(startTime))
		}
		randElement := rand.Intn(len(stations))
		station := stations[randElement]
		data := fmt.Sprint(station.id, ";", station.temperature(randomGenerator), "\n")
		buffered := writer.Buffered()
		if (bufSize - buffered) < 2000 {
			mutexFile.Lock()
			writer.Flush()
			mutexFile.Unlock()
		}
		_, err := writer.WriteString(data)
		if err != nil {
			errCh <- err
			return
		}
	}
	mutexFile.Lock()
	writer.Flush()
	mutexFile.Unlock()
}

func getListOfStations() []*Station {

	stations := []*Station{
		NewStation("Abha", 18.0),
		NewStation("Abidjan", 26.0),
		NewStation("Abéché", 29.4),
		NewStation("Accra", 26.4),
		NewStation("Addis Ababa", 16.0),
		NewStation("Adelaide", 17.3),
		NewStation("Aden", 29.1),
		NewStation("Ahvaz", 25.4),
		NewStation("Albuquerque", 14.0),
		NewStation("Alexandra", 11.0),
		NewStation("Alexandria", 20.0),
		NewStation("Algiers", 18.2),
		NewStation("Alice Springs", 21.0),
		NewStation("Almaty", 10.0),
		NewStation("Amsterdam", 10.2),
		NewStation("Anadyr", -6.9),
		NewStation("Anchorage", 2.8),
		NewStation("Andorra la Vella", 9.8),
		NewStation("Ankara", 12.0),
		NewStation("Antananarivo", 17.9),
		NewStation("Antsiranana", 25.2),
		NewStation("Arkhangelsk", 1.3),
		NewStation("Ashgabat", 17.1),
		NewStation("Asmara", 15.6),
		NewStation("Assab", 30.5),
		NewStation("Astana", 3.5),
		NewStation("Athens", 19.2),
		NewStation("Atlanta", 17.0),
		NewStation("Auckland", 15.2),
		NewStation("Austin", 20.7),
		NewStation("Baghdad", 22.77),
		NewStation("Baguio", 19.5),
		NewStation("Baku", 15.1),
		NewStation("Baltimore", 13.1),
		NewStation("Bamako", 27.8),
		NewStation("Bangkok", 28.6),
		NewStation("Bangui", 26.0),
		NewStation("Banjul", 26.0),
		NewStation("Barcelona", 18.2),
		NewStation("Bata", 25.1),
		NewStation("Batumi", 14.0),
		NewStation("Beijing", 12.9),
		NewStation("Beirut", 20.9),
		NewStation("Belgrade", 12.5),
		NewStation("Belize City", 26.7),
		NewStation("Benghazi", 19.9),
		NewStation("Bergen", 7.7),
		NewStation("Berlin", 10.3),
		NewStation("Bilbao", 14.7),
		NewStation("Birao", 26.5),
		NewStation("Bishkek", 11.3),
		NewStation("Bissau", 27.0),
		NewStation("Blantyre", 22.2),
		NewStation("Bloemfontein", 15.6),
		NewStation("Boise", 11.4),
		NewStation("Bordeaux", 14.2),
		NewStation("Bosaso", 30.0),
		NewStation("Boston", 10.9),
		NewStation("Bouaké", 26.0),
		NewStation("Bratislava", 10.5),
		NewStation("Brazzaville", 25.0),
		NewStation("Bridgetown", 27.0),
		NewStation("Brisbane", 21.4),
		NewStation("Brussels", 10.5),
		NewStation("Bucharest", 10.8),
		NewStation("Budapest", 11.3),
		NewStation("Bujumbura", 23.8),
		NewStation("Bulawayo", 18.9),
		NewStation("Burnie", 13.1),
		NewStation("Busan", 15.0),
		NewStation("Cabo San Lucas", 23.9),
		NewStation("Cairns", 25.0),
		NewStation("Cairo", 21.4),
		NewStation("Calgary", 4.4),
		NewStation("Canberra", 13.1),
		NewStation("Cape Town", 16.2),
		NewStation("Changsha", 17.4),
		NewStation("Charlotte", 16.1),
		NewStation("Chiang Mai", 25.8),
		NewStation("Chicago", 9.8),
		NewStation("Chihuahua", 18.6),
		NewStation("Chișinău", 10.2),
		NewStation("Chittagong", 25.9),
		NewStation("Chongqing", 18.6),
		NewStation("Christchurch", 12.2),
		NewStation("City of San Marino", 11.8),
		NewStation("Colombo", 27.4),
		NewStation("Columbus", 11.7),
		NewStation("Conakry", 26.4),
		NewStation("Copenhagen", 9.1),
		NewStation("Cotonou", 27.2),
		NewStation("Cracow", 9.3),
		NewStation("Da Lat", 17.9),
		NewStation("Da Nang", 25.8),
		NewStation("Dakar", 24.0),
		NewStation("Dallas", 19.0),
		NewStation("Damascus", 17.0),
		NewStation("Dampier", 26.4),
		NewStation("Dar es Salaam", 25.8),
		NewStation("Darwin", 27.6),
		NewStation("Denpasar", 23.7),
		NewStation("Denver", 10.4),
		NewStation("Detroit", 10.0),
		NewStation("Dhaka", 25.9),
		NewStation("Dikson", -11.1),
		NewStation("Dili", 26.6),
		NewStation("Djibouti", 29.9),
		NewStation("Dodoma", 22.7),
		NewStation("Dolisie", 24.0),
		NewStation("Douala", 26.7),
		NewStation("Dubai", 26.9),
		NewStation("Dublin", 9.8),
		NewStation("Dunedin", 11.1),
		NewStation("Durban", 20.6),
		NewStation("Dushanbe", 14.7),
		NewStation("Edinburgh", 9.3),
		NewStation("Edmonton", 4.2),
		NewStation("El Paso", 18.1),
		NewStation("Entebbe", 21.0),
		NewStation("Erbil", 19.5),
		NewStation("Erzurum", 5.1),
		NewStation("Fairbanks", -2.3),
		NewStation("Fianarantsoa", 17.9),
		NewStation("Flores,  Petén", 26.4),
		NewStation("Frankfurt", 10.6),
		NewStation("Fresno", 17.9),
		NewStation("Fukuoka", 17.0),
		NewStation("Gabès", 19.5),
		NewStation("Gaborone", 21.0),
		NewStation("Gagnoa", 26.0),
		NewStation("Gangtok", 15.2),
		NewStation("Garissa", 29.3),
		NewStation("Garoua", 28.3),
		NewStation("George Town", 27.9),
		NewStation("Ghanzi", 21.4),
		NewStation("Gjoa Haven", -14.4),
		NewStation("Guadalajara", 20.9),
		NewStation("Guangzhou", 22.4),
		NewStation("Guatemala City", 20.4),
		NewStation("Halifax", 7.5),
		NewStation("Hamburg", 9.7),
		NewStation("Hamilton", 13.8),
		NewStation("Hanga Roa", 20.5),
		NewStation("Hanoi", 23.6),
		NewStation("Harare", 18.4),
		NewStation("Harbin", 5.0),
		NewStation("Hargeisa", 21.7),
		NewStation("Hat Yai", 27.0),
		NewStation("Havana", 25.2),
		NewStation("Helsinki", 5.9),
		NewStation("Heraklion", 18.9),
		NewStation("Hiroshima", 16.3),
		NewStation("Ho Chi Minh City", 27.4),
		NewStation("Hobart", 12.7),
		NewStation("Hong Kong", 23.3),
		NewStation("Honiara", 26.5),
		NewStation("Honolulu", 25.4),
		NewStation("Houston", 20.8),
		NewStation("Ifrane", 11.4),
		NewStation("Indianapolis", 11.8),
		NewStation("Iqaluit", -9.3),
		NewStation("Irkutsk", 1.0),
		NewStation("Istanbul", 13.9),
		NewStation("İzmir", 17.9),
		NewStation("Jacksonville", 20.3),
		NewStation("Jakarta", 26.7),
		NewStation("Jayapura", 27.0),
		NewStation("Jerusalem", 18.3),
		NewStation("Johannesburg", 15.5),
		NewStation("Jos", 22.8),
		NewStation("Juba", 27.8),
		NewStation("Kabul", 12.1),
		NewStation("Kampala", 20.0),
		NewStation("Kandi", 27.7),
		NewStation("Kankan", 26.5),
		NewStation("Kano", 26.4),
		NewStation("Kansas City", 12.5),
		NewStation("Karachi", 26.0),
		NewStation("Karonga", 24.4),
		NewStation("Kathmandu", 18.3),
		NewStation("Khartoum", 29.9),
		NewStation("Kingston", 27.4),
		NewStation("Kinshasa", 25.3),
		NewStation("Kolkata", 26.7),
		NewStation("Kuala Lumpur", 27.3),
		NewStation("Kumasi", 26.0),
		NewStation("Kunming", 15.7),
		NewStation("Kuopio", 3.4),
		NewStation("Kuwait City", 25.7),
		NewStation("Kyiv", 8.4),
		NewStation("Kyoto", 15.8),
		NewStation("La Ceiba", 26.2),
		NewStation("La Paz", 23.7),
		NewStation("Lagos", 26.8),
		NewStation("Lahore", 24.3),
		NewStation("Lake Havasu City", 23.7),
		NewStation("Lake Tekapo", 8.7),
		NewStation("Las Palmas de Gran Canaria", 21.2),
		NewStation("Las Vegas", 20.3),
		NewStation("Launceston", 13.1),
		NewStation("Lhasa", 7.6),
		NewStation("Libreville", 25.9),
		NewStation("Lisbon", 17.5),
		NewStation("Livingstone", 21.8),
		NewStation("Ljubljana", 10.9),
		NewStation("Lodwar", 29.3),
		NewStation("Lomé", 26.9),
		NewStation("London", 11.3),
		NewStation("Los Angeles", 18.6),
		NewStation("Louisville", 13.9),
		NewStation("Luanda", 25.8),
		NewStation("Lubumbashi", 20.8),
		NewStation("Lusaka", 19.9),
		NewStation("Luxembourg City", 9.3),
		NewStation("Lviv", 7.8),
		NewStation("Lyon", 12.5),
		NewStation("Madrid", 15.0),
		NewStation("Mahajanga", 26.3),
		NewStation("Makassar", 26.7),
		NewStation("Makurdi", 26.0),
		NewStation("Malabo", 26.3),
		NewStation("Malé", 28.0),
		NewStation("Managua", 27.3),
		NewStation("Manama", 26.5),
		NewStation("Mandalay", 28.0),
		NewStation("Mango", 28.1),
		NewStation("Manila", 28.4),
		NewStation("Maputo", 22.8),
		NewStation("Marrakesh", 19.6),
		NewStation("Marseille", 15.8),
		NewStation("Maun", 22.4),
		NewStation("Medan", 26.5),
		NewStation("Mek'ele", 22.7),
		NewStation("Melbourne", 15.1),
		NewStation("Memphis", 17.2),
		NewStation("Mexicali", 23.1),
		NewStation("Mexico City", 17.5),
		NewStation("Miami", 24.9),
		NewStation("Milan", 13.0),
		NewStation("Milwaukee", 8.9),
		NewStation("Minneapolis", 7.8),
		NewStation("Minsk", 6.7),
		NewStation("Mogadishu", 27.1),
		NewStation("Mombasa", 26.3),
		NewStation("Monaco", 16.4),
		NewStation("Moncton", 6.1),
		NewStation("Monterrey", 22.3),
		NewStation("Montreal", 6.8),
		NewStation("Moscow", 5.8),
		NewStation("Mumbai", 27.1),
		NewStation("Murmansk", 0.6),
		NewStation("Muscat", 28.0),
		NewStation("Mzuzu", 17.7),
		NewStation("N'Djamena", 28.3),
		NewStation("Naha", 23.1),
		NewStation("Nairobi", 17.8),
		NewStation("Nakhon Ratchasima", 27.3),
		NewStation("Napier", 14.6),
		NewStation("Napoli", 15.9),
		NewStation("Nashville", 15.4),
		NewStation("Nassau", 24.6),
		NewStation("Ndola", 20.3),
		NewStation("New Delhi", 25.0),
		NewStation("New Orleans", 20.7),
		NewStation("New York City", 12.9),
		NewStation("Ngaoundéré", 22.0),
		NewStation("Niamey", 29.3),
		NewStation("Nicosia", 19.7),
		NewStation("Niigata", 13.9),
		NewStation("Nouadhibou", 21.3),
		NewStation("Nouakchott", 25.7),
		NewStation("Novosibirsk", 1.7),
		NewStation("Nuuk", -1.4),
		NewStation("Odesa", 10.7),
		NewStation("Odienné", 26.0),
		NewStation("Oklahoma City", 15.9),
		NewStation("Omaha", 10.6),
		NewStation("Oranjestad", 28.1),
		NewStation("Oslo", 5.7),
		NewStation("Ottawa", 6.6),
		NewStation("Ouagadougou", 28.3),
		NewStation("Ouahigouya", 28.6),
		NewStation("Ouarzazate", 18.9),
		NewStation("Oulu", 2.7),
		NewStation("Palembang", 27.3),
		NewStation("Palermo", 18.5),
		NewStation("Palm Springs", 24.5),
		NewStation("Palmerston North", 13.2),
		NewStation("Panama City", 28.0),
		NewStation("Parakou", 26.8),
		NewStation("Paris", 12.3),
		NewStation("Perth", 18.7),
		NewStation("Petropavlovsk-Kamchatsky", 1.9),
		NewStation("Philadelphia", 13.2),
		NewStation("Phnom Penh", 28.3),
		NewStation("Phoenix", 23.9),
		NewStation("Pittsburgh", 10.8),
		NewStation("Podgorica", 15.3),
		NewStation("Pointe-Noire", 26.1),
		NewStation("Pontianak", 27.7),
		NewStation("Port Moresby", 26.9),
		NewStation("Port Sudan", 28.4),
		NewStation("Port Vila", 24.3),
		NewStation("Port-Gentil", 26.0),
		NewStation("Portland (OR)", 12.4),
		NewStation("Porto", 15.7),
		NewStation("Prague", 8.4),
		NewStation("Praia", 24.4),
		NewStation("Pretoria", 18.2),
		NewStation("Pyongyang", 10.8),
		NewStation("Rabat", 17.2),
		NewStation("Rangpur", 24.4),
		NewStation("Reggane", 28.3),
		NewStation("Reykjavík", 4.3),
		NewStation("Riga", 6.2),
		NewStation("Riyadh", 26.0),
		NewStation("Rome", 15.2),
		NewStation("Roseau", 26.2),
		NewStation("Rostov-on-Don", 9.9),
		NewStation("Sacramento", 16.3),
		NewStation("Saint Petersburg", 5.8),
		NewStation("Saint-Pierre", 5.7),
		NewStation("Salt Lake City", 11.6),
		NewStation("San Antonio", 20.8),
		NewStation("San Diego", 17.8),
		NewStation("San Francisco", 14.6),
		NewStation("San Jose", 16.4),
		NewStation("San José", 22.6),
		NewStation("San Juan", 27.2),
		NewStation("San Salvador", 23.1),
		NewStation("Sana'a", 20.0),
		NewStation("Santo Domingo", 25.9),
		NewStation("Sapporo", 8.9),
		NewStation("Sarajevo", 10.1),
		NewStation("Saskatoon", 3.3),
		NewStation("Seattle", 11.3),
		NewStation("Ségou", 28.0),
		NewStation("Seoul", 12.5),
		NewStation("Seville", 19.2),
		NewStation("Shanghai", 16.7),
		NewStation("Singapore", 27.0),
		NewStation("Skopje", 12.4),
		NewStation("Sochi", 14.2),
		NewStation("Sofia", 10.6),
		NewStation("Sokoto", 28.0),
		NewStation("Split", 16.1),
		NewStation("St. John's", 5.0),
		NewStation("St. Louis", 13.9),
		NewStation("Stockholm", 6.6),
		NewStation("Surabaya", 27.1),
		NewStation("Suva", 25.6),
		NewStation("Suwałki", 7.2),
		NewStation("Sydney", 17.7),
		NewStation("Tabora", 23.0),
		NewStation("Tabriz", 12.6),
		NewStation("Taipei", 23.0),
		NewStation("Tallinn", 6.4),
		NewStation("Tamale", 27.9),
		NewStation("Tamanrasset", 21.7),
		NewStation("Tampa", 22.9),
		NewStation("Tashkent", 14.8),
		NewStation("Tauranga", 14.8),
		NewStation("Tbilisi", 12.9),
		NewStation("Tegucigalpa", 21.7),
		NewStation("Tehran", 17.0),
		NewStation("Tel Aviv", 20.0),
		NewStation("Thessaloniki", 16.0),
		NewStation("Thiès", 24.0),
		NewStation("Tijuana", 17.8),
		NewStation("Timbuktu", 28.0),
		NewStation("Tirana", 15.2),
		NewStation("Toamasina", 23.4),
		NewStation("Tokyo", 15.4),
		NewStation("Toliara", 24.1),
		NewStation("Toluca", 12.4),
		NewStation("Toronto", 9.4),
		NewStation("Tripoli", 20.0),
		NewStation("Tromsø", 2.9),
		NewStation("Tucson", 20.9),
		NewStation("Tunis", 18.4),
		NewStation("Ulaanbaatar", -0.4),
		NewStation("Upington", 20.4),
		NewStation("Ürümqi", 7.4),
		NewStation("Vaduz", 10.1),
		NewStation("Valencia", 18.3),
		NewStation("Valletta", 18.8),
		NewStation("Vancouver", 10.4),
		NewStation("Veracruz", 25.4),
		NewStation("Vienna", 10.4),
		NewStation("Vientiane", 25.9),
		NewStation("Villahermosa", 27.1),
		NewStation("Vilnius", 6.0),
		NewStation("Virginia Beach", 15.8),
		NewStation("Vladivostok", 4.9),
		NewStation("Warsaw", 8.5),
		NewStation("Washington, D.C.", 14.6),
		NewStation("Wau", 27.8),
		NewStation("Wellington", 12.9),
		NewStation("Whitehorse", -0.1),
		NewStation("Wichita", 13.9),
		NewStation("Willemstad", 28.0),
		NewStation("Winnipeg", 3.0),
		NewStation("Wrocław", 9.6),
		NewStation("Xi'an", 14.1),
		NewStation("Yakutsk", -8.8),
		NewStation("Yangon", 27.5),
		NewStation("Yaoundé", 23.8),
		NewStation("Yellowknife", -4.3),
		NewStation("Yerevan", 12.4),
		NewStation("Yinchuan", 9.0),
		NewStation("Zagreb", 10.7),
		NewStation("Zanzibar City", 26.0),
		NewStation("Zürich", 9.3),
	}

	return stations
}
