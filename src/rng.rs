use rand::{Rng, SeedableRng, rngs::StdRng};

pub fn rng_from_seed(seed: u64) -> StdRng {
    StdRng::seed_from_u64(seed)
}
