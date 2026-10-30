module github.com/manojrevenkar12-web/pms-platform-scaling

go 1.24

// gopkg.in is not reachable from every build host; these are the same sources from GitHub.
replace (
	gopkg.in/check.v1 => github.com/go-check/check v0.0.0-20201130134442-10cb98267c6c
	gopkg.in/yaml.v3 => github.com/go-yaml/yaml v0.0.0-20220527083530-f6f7691b1fde
)

require (
	github.com/lib/pq v1.10.9
	gopkg.in/yaml.v3 v3.0.1
)
