package utils

import (
	"strings"
	"testing"
)

func TestHasYear(t *testing.T) {
	cases := map[string]bool{
		"Zootopia 2.mkv": false,
		"Zootopia 2":     false,
		"1992 2024 1080p BluRay DDP7 1 x264-ZoroSenpai.mkv": true,
		"Movie 2160p 1080p x264":                            false, // resolutions, no year
		"Blade Runner 2049 2017 1080p":                      true,
		"Зверополис 2 / Zootopia 2 [2025, США]":             true,
		"Show.S01E02.720p":                                  false,
		"2012 2009 BluRay":                                  true,
	}
	for in, want := range cases {
		if got := HasYear(in); got != want {
			t.Errorf("HasYear(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestSafeFolderName(t *testing.T) {
	in := "Зверополис 2 / Zootopia 2 (Jared Bush / Byron Howard) [2025, США]"
	got := SafeFolderName(in)
	if got == "" {
		t.Fatal("empty result")
	}
	const reserved = `<>:"/\|?*`
	if strings.ContainsAny(got, reserved) {
		t.Errorf("SafeFolderName(%q) contains a reserved char: %q", in, got)
	}
	if !HasYear(got) {
		t.Errorf("year lost: %q", got)
	}
	t.Logf("sanitized: %q", got)
}
