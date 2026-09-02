use std::io;

fn main() {
    if let Err(error) = provenance_bin::run(
        std::env::args().skip(1),
        io::stdin().lock(),
        io::stdout().lock(),
    ) {
        eprintln!("jj-prov: {error}");
        std::process::exit(2);
    }
}
