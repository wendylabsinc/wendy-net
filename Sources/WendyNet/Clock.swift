// Package-internal clock alias.

#if WendyNetBackendWendyLite
import WendyLite
typealias WendyClock = WendyLite.WendyClock
#elseif WendyNetBackendStandard
typealias WendyClock = ContinuousClock
#endif
