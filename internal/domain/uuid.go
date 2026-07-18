package domain

// ValidUUID reports whether s is a canonical UUID string. Domain identifiers
// are kept as strings to avoid an infrastructure dependency, but untrusted IDs
// still need validation before they reach PostgreSQL's UUID parser.
func ValidUUID(s string) bool {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	for i := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		c := s[i]
		isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
		if !isHex {
			return false
		}
	}
	return true
}
