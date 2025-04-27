package decoders

import (
    "encoding/base64"
    "fmt"
)

// ----------------------------------------------------------------------------
// Register this decoder for MessageID=8, DAC=1, FI=31
// ----------------------------------------------------------------------------

func init() {
    RegisterDecoder(8, 1, 31, decodeASM8)
}

// ----------------------------------------------------------------------------
// decodeASM8
// ----------------------------------------------------------------------------

func decodeASM8(packet map[string]interface{}) (map[string]interface{}, error) {
    // 1) Base64 decode into a 344-byte bit‐array
    rawB64, ok := packet["BinaryData"].(string)
    if !ok {
        return nil, fmt.Errorf("ASM8: missing BinaryData")
    }
    raw, err := base64.StdEncoding.DecodeString(rawB64)
    if err != nil {
        return nil, fmt.Errorf("ASM8: base64 decode: %v", err)
    }
    bits := raw

    // 2) Field offsets after stripping 18-bit header (Spare+DAC+FI)
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

    lon := getSint(bits, O["longitude"][0], O["longitude"][1])
    lat := getSint(bits, O["latitude"][0], O["latitude"][1])
    // no‐value constants from spec
    if lon != 0x6791AC0 && lat != 0x3412140 {
        out["longitude_deg"] = float64(lon) / 60000.0
        out["latitude_deg"] = float64(lat) / 60000.0
    }

    pa := getUint(bits, O["pos_acc"][0], O["pos_acc"][1])
    out["pos_accuracy_high"] = (pa == 0)

    day := getUint(bits, O["utc_day"][0], O["utc_day"][1])
    hr := getUint(bits, O["utc_hour"][0], O["utc_hour"][1])
    mn := getUint(bits, O["utc_min"][0], O["utc_min"][1])
    if day >= 1 && day <= 31 && hr < 24 && mn < 60 {
        out["utc_day"] = day
        out["utc_time"] = fmt.Sprintf("%02d:%02d", hr, mn)
    }

    // ── Meteorological ─────────────────────────────────────────────────────

    // Wind speed & gust
    if ws := getUint(bits, O["wind_spd"][0], O["wind_spd"][1]); ws != 127 {
        if ws == 126 {
            out["wind_speed_kn"] = ">=126"
        } else {
            out["wind_speed_kn"] = ws
        }
    }
    if wg := getUint(bits, O["wind_gust"][0], O["wind_gust"][1]); wg != 127 {
        if wg == 126 {
            out["wind_gust_kn"] = ">=126"
        } else {
            out["wind_gust_kn"] = wg
        }
    }
    if wd := getUint(bits, O["wind_dir"][0], O["wind_dir"][1]); wd != 360 {
        out["wind_dir_deg"] = wd
    }
    if gd := getUint(bits, O["gust_dir"][0], O["gust_dir"][1]); gd != 360 {
        out["gust_dir_deg"] = gd
    }

    // Air temperature, humidity, dew point
    if at := getSint(bits, O["air_temp"][0], O["air_temp"][1]); at != 2047 {
        out["air_temp_C"] = float64(at) / 10.0
    }
    if rh := getUint(bits, O["rel_hum"][0], O["rel_hum"][1]); rh <= 100 {
        out["rel_hum_pct"] = rh
    }
    if dp := getSint(bits, O["dew_pt"][0], O["dew_pt"][1]); dp != 1023 {
        out["dew_point_C"] = float64(dp) / 10.0
    }

    // Air pressure
    if ap := getUint(bits, O["air_pres"][0], O["air_pres"][1]); ap != 511 {
        switch ap {
        case 0:
            out["air_pres_hPa"] = "<=799"
        case 402:
            out["air_pres_hPa"] = ">=1201"
        default:
            out["air_pres_hPa"] = ap + 800
        }
    }

    // Pressure tendency
    if pt := getUint(bits, O["pres_tend"][0], O["pres_tend"][1]); pt <= 2 {
        tendencies := []string{"steady", "decreasing", "increasing"}
        out["pres_tendency"] = tendencies[pt]
    }

    // Visibility (nautical miles)
    if vis := getUint(bits, O["visibility"][0], O["visibility"][1]); vis != 127 {
        if vis == 127 {
            out["visibility_NM"] = ">=12.7"
        } else {
            out["visibility_NM"] = float64(vis) / 10.0
        }
    }

    // ── Hydrographic ────────────────────────────────────────────────────────

    // Water level
    if wl := getSint(bits, O["water_lvl"][0], O["water_lvl"][1]); wl != 4095 {
        out["water_level_m"] = float64(wl)/100.0 - 10.0
    }
    // Water level trend
    if lt := getUint(bits, O["lvl_trend"][0], O["lvl_trend"][1]); lt <= 2 {
        trends := []string{"steady", "rising", "falling"}
        out["level_trend"] = trends[lt]
    }

    // Currents 1–3
    for i := 1; i <= 3; i++ {
        spOff := O[fmt.Sprintf("cur%d_spd", i)]
        drOff := O[fmt.Sprintf("cur%d_dir", i)]
        lvOff, hasLv := O[fmt.Sprintf("cur%d_lvl", i)]

        if sp := getUint(bits, spOff[0], spOff[1]); sp != 255 {
            out[fmt.Sprintf("current%d_speed_kn", i)] = float64(sp) / 10.0
        }
        if dr := getUint(bits, drOff[0], drOff[1]); dr != 360 {
            out[fmt.Sprintf("current%d_dir_deg", i)] = dr
        }
        if hasLv {
            if lv := getUint(bits, lvOff[0], lvOff[1]); lv != 31 {
                out[fmt.Sprintf("current%d_level_m", i)] = lv
            }
        }
    }

    // ── Waves & Swell ───────────────────────────────────────────────────────

    // Waves
    if wh := getUint(bits, O["wave_h"][0], O["wave_h"][1]); wh != 255 {
        out["wave_height_m"] = float64(wh) / 10.0
    }
    if wp := getUint(bits, O["wave_p"][0], O["wave_p"][1]); wp != 63 {
        out["wave_period_s"] = wp
    }
    if wd2 := getUint(bits, O["wave_d"][0], O["wave_d"][1]); wd2 != 360 {
        out["wave_dir_deg"] = wd2
    }

    // Swell
    if sh := getUint(bits, O["swell_h"][0], O["swell_h"][1]); sh != 255 {
        out["swell_height_m"] = float64(sh) / 10.0
    }
    if sp2 := getUint(bits, O["swell_p"][0], O["swell_p"][1]); sp2 != 63 {
        out["swell_period_s"] = sp2
    }
    if sd := getUint(bits, O["swell_d"][0], O["swell_d"][1]); sd != 360 {
        out["swell_dir_deg"] = sd
    }

    // ── Sea State (Beaufort scale + description) ────────────────────────────

    ss := getUint(bits, O["sea_state"][0], O["sea_state"][1])
    desc := ""
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
    out["sea_state"] = map[string]interface{}{
        "scale": ss,
        "desc":  desc,
    }

    // ── Extras ───────────────────────────────────────────────────────────────

    if wt := getSint(bits, O["water_temp"][0], O["water_temp"][1]); wt != 1023 {
        out["water_temp_C"] = float64(wt) / 10.0
    }
    if pr := getUint(bits, O["precip"][0], O["precip"][1]); pr >= 1 && pr <= 6 {
        precips := []string{"", "rain", "thunderstorm", "ice", "snow", "hail", "mixed"}
        out["precipitation"] = precips[pr]
    }
    if sa := getUint(bits, O["salinity"][0], O["salinity"][1]); sa != 511 {
        out["salinity_ppt"] = float64(sa) / 10.0
    }
    if ic := getUint(bits, O["ice_ind"][0], O["ice_ind"][1]); ic <= 1 {
        out["ice_indicator"] = (ic == 1)
    }

    return out, nil
}
