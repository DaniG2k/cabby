//! `cabby` executable. Not implemented yet: exits through the pending marker so
//! external scenarios report *pending*, never a false pass.

fn main() {
    cabby_core::pending!("cli::main")
}
