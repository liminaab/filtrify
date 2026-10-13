package operatorv2

import "time"

func ParseTime(s string) (time.Time, error) {
	t, err := time.Parse("15:04:05.999999999", s)
	if err == nil {
		return t, nil
	}
	t, err = time.Parse("15:04:05", s)
	if err == nil {
		return t, nil
	}
	t, err = time.Parse("15:04", s)
	if err == nil {
		return t, nil
	}
	return time.Time{}, err
}
