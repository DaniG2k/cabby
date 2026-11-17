//! External-behavior scenarios run against a server binary (`SUT_BIN`, or chosen
//! by `CABBY_BACKEND`). They complement translated unit tests by checking the
//! same contracts through the network interface.
//!
//! Scenarios derived from AnyCable (https://github.com/anycable/anycable) at commit
//! 26a1a703a54b870e42f5660c3d0785db2c602764. Copyright 2016-2026 Vladimir
//! Dementyev. MIT License; see compat/licenses/anycable-MIT-LICENSE.

mod api;
mod origin;
