# Databricks SQL Features - Bug Bash Repository

Multi-feature testing repository for Databricks SQL functionality validation and bug discovery.

---

## 📚 Features

### 🔐 [SQL SECURITY DEFINER](./security_definer/)

Comprehensive test suite for SQL stored procedures with `SECURITY DEFINER` impersonation.

**Status**: ✅ Complete  
**Tests**: 78 test cases (100% passing)  
**Documentation**: [security_definer/README.md](./security_definer/README.md)

**Quick Start**:
```bash
cd security_definer
./scripts/setup_local.sh
python scripts/run_tests.py
```

---

### 🚀 [Future Feature 2](./feature-2/)

*Coming soon...*

---

### 🚀 [Future Feature 3](./feature-3/)

*Coming soon...*

---

## 🗂️ Repository Structure

```
DBSQL-features-bug-bash/
│
├── security_definer/          # SQL SECURITY DEFINER feature (✅ Complete)
│   ├── framework/            # Test framework
│   ├── tests/                # 78 test cases
│   ├── scripts/              # Utility scripts
│   ├── sql/                  # SQL definitions
│   ├── docs/                 # Documentation
│   ├── logs/                 # Test results
│   ├── requirements.txt      # Python dependencies
│   ├── env.template          # Config template
│   └── README.md             # Feature documentation
│
├── [feature-2]/               # Future feature directory
│
├── venv/                      # Shared virtual environment (git-ignored)
│
├── .gitignore                 # Git ignore rules
└── README.md                  # This file
```

---

## 🔧 Getting Started

### One-time Setup

```bash
# Clone repository
git clone <repository-url>
cd DBSQL-features-bug-bash

# Create shared virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### Working with a Feature

```bash
# Navigate to feature directory
cd security_definer

# Install feature dependencies
pip install -r requirements.txt

# Follow feature-specific README
cat README.md
```

---

## 🎯 Feature Guidelines

Each feature directory should be self-contained with:

```
feature-name/
├── framework/          # Feature-specific framework code
├── tests/              # Test suites
├── scripts/            # Utility scripts
├── sql/                # SQL definitions (if needed)
├── docs/               # Documentation (git-ignored)
├── logs/               # Test results (git-ignored)
├── requirements.txt    # Python dependencies
├── env.template        # Environment variable template
└── README.md           # Feature documentation
```

---

## 🔒 Security Best Practices

- ✅ No hardcoded credentials in any file
- ✅ Use `.env` files for local development (git-ignored)
- ✅ Use `env.template` for examples (no actual secrets)
- ✅ Use Databricks Secrets for notebook execution
- ✅ Each feature has its own `.env` file

---

## 🤝 Contributing

### Adding a New Feature

1. **Create feature directory**:
   ```bash
   mkdir feature-name
   cd feature-name
   ```

2. **Copy structure from existing feature**:
   ```bash
   cp -r ../security_definer/{framework,tests,scripts} .
   ```

3. **Create feature-specific files**:
   - `README.md` - Feature documentation
   - `requirements.txt` - Dependencies
   - `env.template` - Configuration template

4. **Update top-level README** with new feature

---

## 📖 Available Features

| Feature | Status | Tests | Description |
|---------|--------|-------|-------------|
| [SQL SECURITY DEFINER](./security_definer/) | ✅ Complete | 78 | Stored procedure impersonation testing |
| Feature 2 | 🚧 Planned | - | TBD |
| Feature 3 | 🚧 Planned | - | TBD |

---

## 🎉 Getting Help

- Each feature has its own `README.md` with detailed instructions
- Check `docs/` directory in each feature for additional guides
- Review `env.template` for required configuration

---

**Repository Type**: Multi-feature testing framework  
**Last Updated**: January 30, 2026  
**Maintainer**: Databricks QA Team
