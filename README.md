# Zero-Copy sendmmsg

This crates offer an alternative to `solana-streamer::sendmmsg` module based of io-uring, `SendZc` opcode and registered buffers.