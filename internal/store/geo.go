package store

import "math"

const (
	MIN_LATITUDE  = -85.05112878
	MAX_LATITUDE  = 85.05112878
	MIN_LONGITUDE = -180.0
	MAX_LONGITUDE = 180.0

	LATITUDE_RANGE  = MAX_LATITUDE - MIN_LATITUDE
	LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE
)

func spreadInt32ToInt64(v uint32) uint64 {
	result := uint64(v)
	result = (result | (result << 16)) & 0x0000FFFF0000FFFF
	result = (result | (result << 8)) & 0x00FF00FF00FF00FF
	result = (result | (result << 4)) & 0x0F0F0F0F0F0F0F0F
	result = (result | (result << 2)) & 0x3333333333333333
	result = (result | (result << 1)) & 0x5555555555555555
	return result
}

func interleave(x, y uint32) uint64 {
	xSpread := spreadInt32ToInt64(x)
	ySpread := spreadInt32ToInt64(y)
	yShifted := ySpread << 1
	return xSpread | yShifted
}

func GeoScoreEncode(latitude, longitude float64) uint64 {
	// Normalize to the range 0-2^26
	normalizedLatitude := math.Pow(2, 26) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE
	normalizedLongitude := math.Pow(2, 26) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE

	// Truncate to integers
	latInt := uint32(normalizedLatitude)
	lonInt := uint32(normalizedLongitude)

	return interleave(latInt, lonInt)
}

//

type Coordinates struct {
	Latitude  float64
	Longitude float64
}

func compactInt64ToInt32(v uint64) uint32 {
	result := v & 0x5555555555555555
	result = (result | (result >> 1)) & 0x3333333333333333
	result = (result | (result >> 2)) & 0x0F0F0F0F0F0F0F0F
	result = (result | (result >> 4)) & 0x00FF00FF00FF00FF
	result = (result | (result >> 8)) & 0x0000FFFF0000FFFF
	result = (result | (result >> 16)) & 0x00000000FFFFFFFF
	return uint32(result)
}

func convertGridNumbersToCoordinates(gridLatitudeNumber, gridLongitudeNumber uint32) (lat, lon float64) {
	// Calculate the grid boundaries
	gridLatitudeMin := MIN_LATITUDE + LATITUDE_RANGE*(float64(gridLatitudeNumber)/math.Pow(2, 26))
	gridLatitudeMax := MIN_LATITUDE + LATITUDE_RANGE*(float64(gridLatitudeNumber+1)/math.Pow(2, 26))
	gridLongitudeMin := MIN_LONGITUDE + LONGITUDE_RANGE*(float64(gridLongitudeNumber)/math.Pow(2, 26))
	gridLongitudeMax := MIN_LONGITUDE + LONGITUDE_RANGE*(float64(gridLongitudeNumber+1)/math.Pow(2, 26))

	// Calculate the center point of the grid cell
	latitude := (gridLatitudeMin + gridLatitudeMax) / 2
	longitude := (gridLongitudeMin + gridLongitudeMax) / 2

	return latitude, longitude
}

func GeoScoreDecode(geoCode uint64) (lat, lon float64) {
	// Align bits of both latitude and longitude to take even-numbered position
	y := geoCode >> 1
	x := geoCode

	// Compact bits back to 32-bit ints
	gridLatitudeNumber := compactInt64ToInt32(x)
	gridLongitudeNumber := compactInt64ToInt32(y)

	return convertGridNumbersToCoordinates(gridLatitudeNumber, gridLongitudeNumber)
}

//

func haversine(θ float64) float64 {
	return .5 * (1 - math.Cos(θ))
}

type pos struct {
	φ float64 // latitude, radians
	ψ float64 // longitude, radians
}

func degPos(lat, lon float64) pos {
	return pos{lat * math.Pi / 180, lon * math.Pi / 180}
}

const rEarth = 6372797.560856 // meters

func hsDist(p1, p2 pos) float64 {
	return 2 * rEarth * math.Asin(math.Sqrt(haversine(p2.φ-p1.φ)+
		math.Cos(p1.φ)*math.Cos(p2.φ)*haversine(p2.ψ-p1.ψ)))
}

func GetDistance(lat1, lat2, lon1, lon2 float64) float64 {
	return hsDist(degPos(lat1, lon1), degPos(lat2, lon2))
}
