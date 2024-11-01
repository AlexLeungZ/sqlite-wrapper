from functools import cache


@cache
# SQL statements generator for equal expressions
def val_eq(val: str | bool | float) -> str:
    """expression == val"""
    return f"== '{val}'"


@cache
# SQL statements generator for not equal expressions
def val_neq(val: str | bool | float) -> str:
    """expression != val"""
    return f"!= '{val}'"


@cache
# SQL statements generator for less than expressions
def val_lt(val: float) -> str:
    """expression < val"""
    return f"< '{val}'"


@cache
# SQL statements generator for greater than expressions
def val_gt(val: float) -> str:
    """expression > val"""
    return f"> '{val}'"


@cache
# SQL statements generator for less than or equal expressions
def val_lte(val: float) -> str:
    """expression <= val"""
    return f"<= '{val}'"


@cache
# SQL statements generator for greater than or equal expressions
def val_gte(val: float) -> str:
    """expression >= val"""
    return f">= '{val}'"


@cache
# SQL statements generator for (not) in expressions
def val_in(*args: str | float, negate: bool = False) -> str:
    """expression IN (value1, value2, .... value_n), where n > 1"""
    return f"{'NOT ' if negate else ''}IN {args}"


@cache
# SQL statements generator for value (not) between expressions
def val_btw(lower: str | float, upper: str | float, negate: bool = False) -> str:
    """expression BETWEEN lower AND upper"""
    return f"{'NOT ' if negate else ''}BETWEEN '{lower}' AND '{upper}'"


@cache
# SQL statements generator for (not) like expressions
def val_like(pattern: str, negate: bool = False) -> str:
    """expression LIKE pattern"""
    return f"{'NOT ' if negate else ''}LIKE '{pattern}'"


@cache
# SQL statements generator for is (not) null expressions
def val_null(negate: bool = False) -> str:
    """expression IS NULL"""
    return f"IS {'NOT ' if negate else ''}NULL"
