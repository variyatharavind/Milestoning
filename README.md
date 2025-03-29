# Python Project

This is a Python project with a standard structure for source code and tests.

## Project Structure

```
.
├── src/           # Source code
├── tests/         # Test files
├── requirements.txt
└── README.md
```

## Setup

1. Create a virtual environment:
   ```bash
   python -m venv venv
   ```

2. Activate the virtual environment:
   - Windows:
     ```bash
     .\venv\Scripts\activate
     ```
   - Unix/MacOS:
     ```bash
     source venv/bin/activate
     ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Development

- Source code goes in the `src` directory
- Tests go in the `tests` directory
- Use `pytest` to run tests
- Use `black` for code formatting
- Use `flake8` for linting 