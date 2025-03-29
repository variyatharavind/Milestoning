"""
Tests for mathematical operations.
"""
import pytest
from src.math import add_numbers

def test_add_numbers_positive():
    """Test adding two positive numbers."""
    assert add_numbers(2, 3) == 5
    assert add_numbers(10, 20) == 30

def test_add_numbers_negative():
    """Test adding negative numbers."""
    assert add_numbers(-1, -1) == -2
    assert add_numbers(-5, 3) == -2

def test_add_numbers_zero():
    """Test adding with zero."""
    assert add_numbers(0, 5) == 5
    assert add_numbers(0, 0) == 0

def test_add_numbers_float():
    """Test adding floating point numbers."""
    assert add_numbers(1.5, 2.5) == 4.0
    assert add_numbers(0.1, 0.2) == pytest.approx(0.3)  # Using pytest.approx for float comparison 