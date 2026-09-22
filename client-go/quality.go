package client

const qualityClassMask uint32 = 0xc0

func qualityIsGood(quality uint32) bool { return quality&qualityClassMask == 0xc0 }

// qualityDescription decodes standard OPC DA class and substatus bits.
func qualityDescription(quality uint32) string {
	switch quality & 0xfc {
	case 0x00:
		return "Bad (non-specific)"
	case 0x04:
		return "Bad (configuration error)"
	case 0x08:
		return "Bad (not connected)"
	case 0x0c:
		return "Bad (device failure)"
	case 0x10:
		return "Bad (sensor failure)"
	case 0x14:
		return "Bad (last known value)"
	case 0x18:
		return "Bad (communication failure)"
	case 0x1c:
		return "Bad (out of service)"
	case 0x20:
		return "Bad (waiting for initial value)"
	case 0x44:
		return "Uncertain (last usable value)"
	case 0x50:
		return "Uncertain (sensor not accurate)"
	case 0x54:
		return "Uncertain (engineering units exceeded)"
	case 0x58:
		return "Uncertain (sub-normal)"
	case 0xd8:
		return "Good (local override)"
	}
	switch quality & qualityClassMask {
	case 0xc0:
		return "Good"
	case 0x00:
		return "Bad"
	case 0x40:
		return "Uncertain"
	default:
		return "Reserved quality"
	}
}
