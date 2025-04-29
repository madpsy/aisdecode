package decoders

import (
    "encoding/base64"
    "fmt"
)

// ----------------------------------------------------------------------------
// Register this decoder for MessageID=8, DAC=1, FI=31
// ----------------------------------------------------------------------------

func init() {
    RegisterDecoder(8, 1, 31, decode_8_1_31)
}

// https://www.e-navigation.nl/content/meteorological-and-hydrographic-data

// ----------------------------------------------------------------------------
// decode_8_1_31 (using SafeGetUint / SafeGetSint)
// ----------------------------------------------------------------------------

func decode_8_1_31(packet map[string]interface{}) (map[string]interface{}, error) {
    // 1) Base64 decode into raw bytes
    rawB64, ok := packet["BinaryData"].(string)
    if !ok {
        return nil, fmt.Errorf("decode_8_1_31: missing BinaryData")
    }
    raw, err := base64.StdEncoding.DecodeString(rawB64)
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31: base64 decode: %v", err)
    }

    // 2) Use raw as bit-slice (1 byte == 1 bit)
    bits := raw

    // 3) Field offsets after stripping 56-bit header (Spare+DAC+FI etc)
    O := map[string][2]int{
        "longitude":  {0, 25},   "latitude":   {25, 24},
        "pos_acc":    {49, 1},   "utc_day":    {50, 5},
        "utc_hour":   {55, 5},   "utc_min":    {60, 6},
        "wind_spd":   {66, 7},   "wind_gust":  {73, 7},
        "wind_dir":   {80, 9},   "gust_dir":   {89, 9},
        "air_temp":   {98, 11},  "rel_hum":    {109, 7},
        "dew_pt":     {116, 10}, "air_pres":   {126, 9},
        "pres_tend":  {135, 2},  "visibility": {137, 8},
        "water_lvl":  {145, 12}, "lvl_trend":  {157, 2},
        "cur1_spd":   {159, 8},  "cur1_dir":   {167, 9},
        "cur2_spd":   {176, 8},  "cur2_dir":   {184, 9},
        "cur2_lvl":   {193, 5},  "cur3_spd":   {198, 8},
        "cur3_dir":   {206, 9},  "cur3_lvl":   {215, 5},
        "wave_h":     {220, 8},  "wave_p":     {228, 6},
        "wave_d":     {234, 9},  "swell_h":    {243, 8},
        "swell_p":    {251, 6},  "swell_d":    {257, 9},
        "sea_state":  {266, 4},  "water_temp": {270, 10},
        "precip":     {280, 3},  "salinity":   {283, 9},
        "ice_ind":    {292, 2},
    }

    out := make(map[string]interface{})

    // ── Position & UTC ──────────────────────────────────────────────────────

    lonRaw, err := SafeGetSint(bits, O["longitude"][0], O["longitude"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 longitude: %v", err)
    }
    latRaw, err := SafeGetSint(bits, O["latitude"][0], O["latitude"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 latitude: %v", err)
    }
    // no‐value constants from spec
    if lonRaw != 0x6791AC0 && latRaw != 0x3412140 {
        out["Longitude_deg"] = float64(lonRaw) / 60000.0
        out["Latitude_deg"] = float64(latRaw) / 60000.0
    }

    pa, err := SafeGetUint(bits, O["pos_acc"][0], O["pos_acc"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 pos_acc: %v", err)
    }
    out["Position_Accuracy_High"] = (pa == 0)

    day, err := SafeGetUint(bits, O["utc_day"][0], O["utc_day"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 utc_day: %v", err)
    }
    hr, err := SafeGetUint(bits, O["utc_hour"][0], O["utc_hour"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 utc_hour: %v", err)
    }
    mn, err := SafeGetUint(bits, O["utc_min"][0], O["utc_min"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 utc_min: %v", err)
    }
    if day >= 1 && day <= 31 && hr < 24 && mn < 60 {
        out["UTC_Day"] = day
        out["UTC_Time"] = fmt.Sprintf("%02d:%02d", hr, mn)
    }

    // ── Meteorological ─────────────────────────────────────────────────────

    // Wind speed & gust
    ws, err := SafeGetUint(bits, O["wind_spd"][0], O["wind_spd"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 wind_spd: %v", err)
    }
    if ws != 127 {
        if ws == 126 {
            out["Wind_Speed_kn"] = ">=126"
        } else {
            out["Wind_Speed_kn"] = ws
        }
    }

    wg, err := SafeGetUint(bits, O["wind_gust"][0], O["wind_gust"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 wind_gust: %v", err)
    }
    if wg != 127 {
        if wg == 126 {
            out["Wind_Gust_kn"] = ">=126"
        } else {
            out["Wind_Gust_kn"] = wg
        }
    }

    wd, err := SafeGetUint(bits, O["wind_dir"][0], O["wind_dir"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 wind_dir: %v", err)
    }
    if wd != 360 {
        out["Wind_Direction_deg"] = wd
    }

    gd, err := SafeGetUint(bits, O["gust_dir"][0], O["gust_dir"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 gust_dir: %v", err)
    }
    if gd != 360 {
        out["Gust_Direction_deg"] = gd
    }

    // Air temperature, humidity, dew point
    at, err := SafeGetSint(bits, O["air_temp"][0], O["air_temp"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 air_temp: %v", err)
    }
    if at != 2047 {
        out["Air_Temp_C"] = float64(at) / 10.0
    }

    rh, err := SafeGetUint(bits, O["rel_hum"][0], O["rel_hum"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 rel_hum: %v", err)
    }
    if rh <= 100 {
        out["Relative_Humidity_pct"] = rh
    }

    dp, err := SafeGetSint(bits, O["dew_pt"][0], O["dew_pt"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 dew_pt: %v", err)
    }
    if dp != 1023 {
        out["Dew_Point_C"] = float64(dp) / 10.0
    }

    // Air pressure
    ap, err := SafeGetUint(bits, O["air_pres"][0], O["air_pres"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 air_pres: %v", err)
    }
    if ap != 511 {
        switch ap {
        case 0:
            out["Air_Pressure_hPa"] = "<=799"
        case 402:
            out["Air_Pressure_hPa"] = ">=1201"
        default:
            out["Air_Pressure_hPa"] = ap + 800
        }
    }

    // Pressure tendency
    pt, err := SafeGetUint(bits, O["pres_tend"][0], O["pres_tend"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 pres_tend: %v", err)
    }
    if pt <= 2 {
        tendencies := []string{"steady", "decreasing", "increasing"}
        out["Pressure_Tendency"] = tendencies[pt]
    }

    // Visibility (nautical miles)
    vis, err := SafeGetUint(bits, O["visibility"][0], O["visibility"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 visibility: %v", err)
    }
    if vis != 127 {
        if vis == 127 {
            out["Visibility_NM"] = ">=12.7"
        } else {
            out["Visibility_NM"] = float64(vis) / 10.0
        }
    }

    // ── Hydrographic ────────────────────────────────────────────────────────

    // Water level
    wl, err := SafeGetSint(bits, O["water_lvl"][0], O["water_lvl"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 water_lvl: %v", err)
    }
    if wl != 4095 {
        out["Water_Level_m"] = float64(wl)/100.0 - 10.0
    }

    // Water level trend
    lt, err := SafeGetUint(bits, O["lvl_trend"][0], O["lvl_trend"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 lvl_trend: %v", err)
    }
    if lt <= 2 {
        trends := []string{"steady", "rising", "falling"}
        out["Level_Trend"] = trends[lt]
    }

    // Currents 1–3
    for i := 1; i <= 3; i++ {
        spOff := O[fmt.Sprintf("cur%d_spd", i)]
        drOff := O[fmt.Sprintf("cur%d_dir", i)]
        lvOff, hasLv := O[fmt.Sprintf("cur%d_lvl", i)]

        sp, err := SafeGetUint(bits, spOff[0], spOff[1])
        if err != nil {
            return nil, fmt.Errorf("decode_8_1_31 cur%d_spd: %v", i, err)
        }
        if sp != 255 {
            out[fmt.Sprintf("Current_%d_Speed_kn", i)] = float64(sp) / 10.0
        }

        dr, err := SafeGetUint(bits, drOff[0], drOff[1])
        if err != nil {
            return nil, fmt.Errorf("decode_8_1_31 cur%d_dir: %v", i, err)
        }
        if dr != 360 {
            out[fmt.Sprintf("Current_%d_Direction_deg", i)] = dr
        }

        if hasLv {
            lv, err := SafeGetUint(bits, lvOff[0], lvOff[1])
            if err != nil {
                return nil, fmt.Errorf("decode_8_1_31 cur%d_lvl: %v", i, err)
            }
            if lv != 31 {
                out[fmt.Sprintf("Current_%d_Level_m", i)] = lv
            }
        }
    }

    // ── Waves & Swell ───────────────────────────────────────────────────────

    wh, err := SafeGetUint(bits, O["wave_h"][0], O["wave_h"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 wave_h: %v", err)
    }
    if wh != 255 {
        out["Wave_Height_m"] = float64(wh) / 10.0
    }

    wp, err := SafeGetUint(bits, O["wave_p"][0], O["wave_p"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 wave_p: %v", err)
    }
    if wp != 63 {
        out["Wave_Period_s"] = wp
    }

    wd2, err := SafeGetUint(bits, O["wave_d"][0], O["wave_d"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 wave_d: %v", err)
    }
    if wd2 != 360 {
        out["Wave_Direction_deg"] = wd2
    }

    sh, err := SafeGetUint(bits, O["swell_h"][0], O["swell_h"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 swell_h: %v", err)
    }
    if sh != 255 {
        out["Swell_Height_m"] = float64(sh) / 10.0
    }

    sp2, err := SafeGetUint(bits, O["swell_p"][0], O["swell_p"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 swell_p: %v", err)
    }
    if sp2 != 63 {
        out["Swell_Period_s"] = sp2
    }

    sd, err := SafeGetUint(bits, O["swell_d"][0], O["swell_d"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 swell_d: %v", err)
    }
    if sd != 360 {
        out["Swell_Direction_deg"] = sd
    }

    // ── Sea State ────────────────────────────────────────────────────────────

    ss, err := SafeGetUint(bits, O["sea_state"][0], O["sea_state"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 sea_state: %v", err)
    }
    var desc string
    switch ss {
    case 0:
        desc = "Flat"
    case 1:
        desc = "Ripples without crests"
    case 2:
        desc = "Small wavelets; crests glassy, not breaking"
    case 3:
        desc = "Large wavelets; crests begin to break; scattered whitecaps"
    case 4:
        desc = "Small waves"
    case 5:
        desc = "Moderate (1.2 m) longer waves; some foam and spray"
    case 6:
        desc = "Large waves with foam crests and some spray"
    case 7:
        desc = "Sea heaps up and foam begins to streak"
    case 8:
        desc = "Moderately high waves with breaking crests; spindrift"
    case 9:
        desc = "High waves (6-7 m) with dense foam; wave crests roll over"
    case 10:
        desc = "Very high waves; sea white with tumbling; reduced visibility"
    case 11:
        desc = "Exceptionally high waves"
    case 12:
        desc = "Huge waves; air filled with foam and spray; sea white"
    case 13:
        desc = "Not available"
    default:
        desc = "Reserved/future use"
    }
    out["Sea_State"] = map[string]interface{}{
        "Scale":       ss,
        "Description": desc,
    }

    // ── Extras ───────────────────────────────────────────────────────────────

    wt, err := SafeGetSint(bits, O["water_temp"][0], O["water_temp"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 water_temp: %v", err)
    }
    if wt != 1023 {
        out["Water_Temp_C"] = float64(wt) / 10.0
    }

    pr, err := SafeGetUint(bits, O["precip"][0], O["precip"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 precip: %v", err)
    }
    if pr >= 1 && pr <= 6 {
        precips := []string{"", "rain", "thunderstorm", "ice", "snow", "hail", "mixed"}
        out["Precipitation"] = precips[pr]
    }

    sa, err := SafeGetUint(bits, O["salinity"][0], O["salinity"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 salinity: %v", err)
    }
    if sa != 511 {
        out["Salinity_ppt"] = float64(sa) / 10.0
    }

    ic, err := SafeGetUint(bits, O["ice_ind"][0], O["ice_ind"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_31 ice_ind: %v", err)
    }
    if ic <= 1 {
        out["Ice_Indicator"] = (ic == 1)
    }

    return out, nil
}
