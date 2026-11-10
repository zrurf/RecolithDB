use recolith_backend_memory::MemoryBackend;

#[test]
fn memory_backend_satisfies_contract() {
    let backend = MemoryBackend::new();

    recolith_backend_conformance::run_all(&backend);
}
