//! Pando on Linux: the ublk frontend and the `DeviceSwitch` port over device-mapper and loop
//! devices. Empty on other systems.
//!
//! The ublk queue threads run libublk's per-queue runtime. A read the queue can serve from a
//! local image or the cache without waiting is served there; every other request goes to the
//! engine's shared runtime, and its completion comes back through an eventfd the queue polls,
//! because libublk sees a wake from another thread only at its next completion.

#![cfg(target_os = "linux")]
