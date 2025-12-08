# Contributing to Spt

Thank you for your interest in contributing to Spt! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Build System](#build-system)
- [Making Changes](#making-changes)
- [Testing](#testing)
- [Submitting Changes](#submitting-changes)
- [Style Guide](#style-guide)

## Code of Conduct

This project adheres to a Code of Conduct that all contributors are expected to follow. Please be respectful and professional in all interactions.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone <your-fork-url>`
3. Add upstream remote: `git remote add upstream git@github.com:dell/storage-performance-tool.git`
4. Create a feature branch: `git checkout -b feature/your-feature-name`

## Development Setup

### Prerequisites

- Java 21 or higher (required)
- Git (required)
- Docker (optional, for containerized testing)
- A Java IDE (recommended: IntelliJ IDEA, Eclipse, or VS Code with Java extensions)

### IDE Setup

Most modern Java IDEs will automatically recognize the Gradle project structure:

1. **IntelliJ IDEA**: File → Open → Select the project root directory
2. **Eclipse**: File → Import → Gradle → Existing Gradle Project
3. **VS Code**: Open the folder and install the Java Extension Pack

The IDE should automatically use the Gradle Wrapper (`gradlew`) for building and running tests.

## Build System

### Primary Build Tool: Gradle Wrapper

This project uses Gradle as its build system, accessed through the Gradle Wrapper. This ensures all contributors use the same Gradle version.

**All pull requests must build successfully using the Gradle Wrapper.**

```bash
# Unix/Linux/macOS
./gradlew clean build

# Windows
gradlew.bat clean build

# Common commands
./gradlew build          # Build the project
./gradlew test           # Run tests
./gradlew spotlessCheck  # Check code formatting
./gradlew spotlessApply  # Fix code formatting
```

### Optional: Makefile

A Makefile is provided as a convenience wrapper around Gradle commands. It is **not** required for contributions:

```bash
make build   # Equivalent to ./gradlew build -x test
make test    # Equivalent to ./gradlew test
make clean   # Equivalent to ./gradlew clean
```

**Important**: 
- The Makefile is Unix-only and provided for developer convenience
- CI/CD pipelines use `gradlew` directly
- Pull requests should not depend on Makefile-specific functionality

## Making Changes

### Workflow

1. Ensure your fork is up to date:
   ```bash
   git checkout main
   git pull upstream main
   git push origin main
   ```

2. Create a feature branch:
   ```bash
   git checkout -b feature/descriptive-name
   ```

3. Make your changes following the style guide

4. Build and test locally:
   ```bash
   ./gradlew clean build
   ```

5. Commit your changes:
   ```bash
   git add .
   git commit -m "feat: add new feature X"
   ```

### Commit Message Format

We follow conventional commits format:

- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `style:` Code style changes (formatting, etc.)
- `refactor:` Code refactoring
- `test:` Test additions or modifications
- `chore:` Build process or auxiliary tool changes

## Testing

### Running Tests

```bash
# Run all tests
./gradlew test

# Run specific test class
./gradlew test --tests "com.dell.spt.base.MyTestClass"

# Run with specific log level
./gradlew test -i  # info level
./gradlew test -d  # debug level
```

### Writing Tests

- All new features should include tests
- Bug fixes should include a test that reproduces the issue
- Maintain or improve code coverage
- Use JUnit for unit tests
- Use Robot Framework for integration tests (where applicable)

## Submitting Changes

1. Ensure all tests pass: `./gradlew clean build`

2. Check code formatting: `./gradlew spotlessCheck`
   - Fix any issues with: `./gradlew spotlessApply`

3. Push to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

4. Create a Pull Request:
   - Use a clear, descriptive title
   - Reference any related issues
   - Provide a detailed description of changes
   - Ensure CI builds pass

### Pull Request Checklist

- [ ] Code builds without warnings using `./gradlew clean build`
- [ ] All tests pass
- [ ] Code follows the project style guide (Spotless check passes)
- [ ] Documentation is updated if needed
- [ ] Commit messages follow conventional format
- [ ] PR description clearly explains the changes

## Style Guide

### Java Code Style

This project uses Spotless with Eclipse formatting rules for consistent code style:

```bash
# Check formatting
./gradlew spotlessCheck

# Auto-format code
./gradlew spotlessApply
```

Key style points:
- Use tabs for indentation
- Maximum line length: 120 characters
- Use meaningful variable and method names
- Add Javadoc for public APIs
- Follow standard Java naming conventions

### General Guidelines

- Keep methods focused and small
- Write self-documenting code
- Add comments only when necessary to explain "why", not "what"
- Maintain backward compatibility when possible
- Update documentation for API changes

## Questions?

If you have questions about contributing:

1. Check existing issues and pull requests
2. Review the project documentation
3. Open a discussion issue for larger changes
4. Contact the maintainers

Thank you for contributing to Spt!