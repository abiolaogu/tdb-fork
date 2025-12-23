# Contributing to LumaDB

We welcome contributions to LumaDB! Please follow these guidelines to ensure a smooth collaboration.

## Getting Started

1.  **Fork the repository** on GitHub.
2.  **Clone your fork** locally:
    ```bash
    git clone https://github.com/yourusername/lumadb.git
    cd lumadb
    ```
3.  **Install dependencies**:
    - Rust (stable)
    - Go (1.20+)
    - Docker

## Workflow

1.  **Create a branch** for your feature or fix:
    ```bash
    git checkout -b feature/my-new-feature
    ```
2.  **Make your changes**.
3.  **Run tests** to ensure no regressions:
    ```bash
    # Rust Core
    cd rust-core
    cargo test
    cargo clippy -- -D warnings
    
    # Go Cluster
    cd ../go-cluster
    go test ./...
    ```
4.  **Format your code**:
    ```bash
    cargo fmt
    go fmt ./...
    ```
5.  **Commit your changes** with a descriptive message.
6.  **Push to your fork** and submit a Pull Request.

## Code Style

- **Rust**: Follow standard Rust idioms. Use `cargo fmt`.
- **Go**: Follow standard Go conventions (Effective Go). Use `go fmt`.

## Reporting Issues

If you find a bug or have a request, please open an issue on GitHub with reproduction steps and relevant logs.

## License

By contributing, you agree that your contributions will be licensed under the project's [LICENSE](LICENSE) file.
