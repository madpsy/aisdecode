# aisdecode
AIS Decoder and Web based Tracker for serial (and USB) NMEA hardware. Tested with ia SevenStar 2rxPro.

1) Clone this repo
2) Download prebuilt binary from [Releases](https://github.com/madpsy/aisdecode/releases) for your OS/Arch (or build your own)
3) Run the program from within the cloned repo directory (chmod +x it first on Linux/Mac)
4) In a browser go to http://127.0.0.1:8100 (or whatever IP address of the host)

```
Usage of aisdecode:
  -baud int
    	Baud rate (default: 38400)
  -debug
    	Enable debug output
  -serial-port string
    	Serial port device (default: /dev/ttyUSB0)
  -show-decodes
    	Output the decoded messages
  -web-root string
    	Web root directory (default: current directory)
  -ws-port int
    	WebSocket port (default: 8100)
```

![aisdecode](images/aisdecode.png)
