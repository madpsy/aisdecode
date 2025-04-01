# aisdecode
AIS Decoder and Web based Tracker for serial (and USB) NMEA hardware. Tested with a SevenStar 2rxPro.

1) Clone this repo
2) Download prebuilt binary from [Releases](https://github.com/madpsy/aisdecode/releases) for your OS/Arch (or build your own)
3) Run the program from within the cloned repo directory (chmod +x first on Linux/Mac)
4) In a browser go to http://127.0.0.1:8100 (or whatever IP address of the host)

```
Usage of aisdecode:
  -aggregator string
    	Comma delimited list of aggregator host/ip:port (optional)
  -aggregator-public-url string
    	Public aggregator URL to push myinfo.json to on startup (optional)
  -baud int
    	Baud rate (default: 38400), ignored if -serial-port is not specified (default 38400)
  -debug
    	Enable debug output
  -dedupe-window int
    	Deduplication window in milliseconds (default: 1000, set to 0 to disable deduplication) (default 1000)
  -dump-vessel-data
    	Log the latest vessel data to the screen whenever it is updated
  -expire-after duration
    	Expire vessel data if no update is received within this duration (default: 24h)
  -no-state
    	When specified, do not save or load the state (default: false)
  -serial-port string
    	Serial port device (optional)
  -show-decodes
    	Output the decoded messages
  -state-dir string
    	Directory to store state (optional). Overrides the default location of web-root
  -udp-listen-port int
    	UDP listen port for incoming NMEA data (default: 8101)
  -update-interval int
    	Update interval in seconds for emitting latest vessel data (default: 10)
  -web-root string
    	Web root directory (default: current directory)
  -ws-port int
    	WebSocket port (default: 8100)
```

![aisdecode](images/aisdecode.png)
