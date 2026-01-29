# SQL SECURITY DEFINER - Bug Bash Test Suite

Comprehensive test suite for Databricks SQL stored procedures with `SQL SECURITY DEFINER` impersonation feature.

## 📊 Overview

**Total Tests**: 78 comprehensive test cases  
**Coverage**: Core impersonation, security, Unity Catalog, context switching, Jobs API  
**Status**: ✅ 100% passing

### Test Categories

| Category | Tests | Description |
|----------|-------|-------------|
| Core Impersonation | 3 | Basic DEFINER vs INVOKER identity resolution |
| Object Access | 3 | Read/write operations, gateway patterns |
| Nested Procedures | 2 | Multi-level procedure calls |
| Security & Injection | 2 | SQL injection prevention, dynamic SQL |
| Error Handling | 2 | Error messages, exception handling |
| Unity Catalog | 4 | UC permissions, cross-schema access |
| Negative Cases | 3 | Unauthorized access, abuse prevention |
| Compliance | 1 | Regression testing |
| Known Issues | 5 | Documented limitations |
| **Advanced Tests** | 28 | Concurrency, privilege escalation, deep nesting |
| **Jobs API Tests** | 25 | Serverless compute, bidirectional context switching |

---

## 🚀 Quick Start

```bash
# From repository root
cd security_definer

# Automated setup
./scripts/setup_local.sh

# Or manual setup
python3 -m venv ../venv
source ../venv/bin/activate
pip install -r ../requirements.txt

# Configure environment
cp env.template .env
# Edit .env with your credentials

# Export variables
export $(cat .env | xargs)

# Test connectivity
python tests/utils/test_connection.py

# Run all tests
python scripts/run_tests.py
```

---

## 📁 Project Structure

```
security_definer/
├── framework/          # Test framework
├── tests/             # Test suites (78 tests)
├── scripts/           # Utility scripts  
├── sql/               # SQL definitions
├── docs/              # Documentation (git-ignored)
├── logs/              # Test results (git-ignored)
└── env.template       # Environment template
```

For detailed documentation, see `docs/` directory or the original comprehensive README.

---

## 🧪 Running Tests

```bash
# All tests
python scripts/run_tests.py

# Specific connectivity test
python tests/utils/test_connection.py

# Serverless tests
python scripts/run_serverless_tests.py
```

---

## 🔒 Security

- ✅ No hardcoded credentials
- ✅ Uses environment variables
- ✅ Databricks Secrets for notebooks
- ✅ All sensitive data in .env (git-ignored)

---

## 📚 Documentation

See `docs/` directory for detailed guides:
- `LOCAL_SETUP.md` - Local development setup
- `SERVERLESS_TESTING_GUIDE.md` - Serverless compute testing
- `JOBS_API_COMPLETE_TESTING.md` - Jobs API integration
- `GIT_SECURITY.md` - Security best practices

---

**Feature Status**: ✅ Complete  
**Last Updated**: January 30, 2026
