from __future__ import annotations  # noqa: CPY001

from typing import TYPE_CHECKING

import sqlglot
from sqlglot import expressions as exp

if TYPE_CHECKING:
    from collections.abc import Iterable


class OrderBy:
    """Order by clause."""

    def __init__(self, field: str, *, descending: bool | None = None) -> None:
        self._field = field
        self._descending = descending

    def sql(self) -> str:
        result = self._field
        if self._descending is not None:
            result += " DESC" if self._descending else " ASC"
        return result


class GAQL:
    """Google Ads Query Language (GAQL) query builder.

    Encapsulates GAQL query options including selected fields, WHERE clause,
    ORDER BY clause, and source table.
    """

    def __init__(
        self,
        *select_fields: str,
        from_table: str,
        where_clause: list[str] | None = None,
        order_by: list[OrderBy] | None = None,
    ) -> None:
        """Initialize GAQL query builder.

        Args:
            select_fields: List of fields to select or comma-separated string
            from_table: Table name to query from
            where_clause: WHERE clause conditions (without WHERE keyword)
            order_by: ORDER BY clause (without ORDER BY keyword)
        """
        self.select_fields = self._normalize_fields(select_fields)
        self.from_table = from_table
        self._where_clauses = where_clause or []
        self._order_by = order_by or []

    def _normalize_fields(self, fields: Iterable[str]) -> list[str]:  # noqa: PLR6301
        return [field.strip() for field in fields]

    def select(self, *fields: str) -> GAQL:
        """Add fields to SELECT clause.

        Args:
            *fields: Field names to select

        Returns:
            self: For method chaining
        """
        self.select_fields.extend(self._normalize_fields(fields))
        return self

    def from_(self, table: str) -> GAQL:
        """Set FROM table.

        Args:
            table: Table name

        Returns:
            self: For method chaining
        """
        self.from_table = table
        return self

    def where(self, condition: str) -> GAQL:
        """Set WHERE clause.

        Args:
            condition: WHERE condition (without WHERE keyword)

        Returns:
            self: For method chaining
        """
        self._where_clauses.append(condition)
        return self

    def order_by(self, clause: OrderBy) -> GAQL:
        """Set ORDER BY clause.

        Args:
            clause: ORDER BY clause (without ORDER BY keyword)

        Returns:
            self: For method chaining
        """
        self._order_by.append(clause)
        return self

    @classmethod
    def from_string(cls, query: str) -> GAQL:
        """Create GAQL from string.

        Args:
            query: GAQL query string
        """
        parsed = sqlglot.parse_one(query)
        if not isinstance(parsed, exp.Select):
            raise ValueError("Query is not a SELECT statement")

        fields = [field.sql() for field in parsed.expressions]
        table = parsed.find(exp.Table)
        if not table:
            raise ValueError("FROM table is not set")

        where = [where_expr.this.sql() for where_expr in parsed.find_all(exp.Where)] if parsed.find_all(exp.Where) else None

        order_by = []
        for ordered_expr in parsed.find_all(exp.Ordered):
            desc = ordered_expr.args.get("desc")
            column = ordered_expr.this.sql()
            order_by.append(OrderBy(column, descending=desc))

        return cls(
            *fields,
            from_table=table.sql(),
            where_clause=where,
            order_by=order_by,
        )

    def __str__(self) -> str:
        """Convert to GAQL query string.

        Returns:
            str: Complete GAQL query

        Raises:
            ValueError: If SELECT fields or FROM table are not set
        """
        if not self.select_fields:
            msg = "SELECT fields are required"
            raise ValueError(msg)
        if not self.from_table:
            msg = "FROM table is required"
            raise ValueError(msg)

        query_parts = []

        # SELECT clause
        select_clause = "SELECT " + ", ".join(self.select_fields)
        query_parts.append(select_clause)

        # FROM clause
        from_clause = f"FROM {self.from_table}"
        query_parts.append(from_clause)

        # WHERE clause
        if self._where_clauses:
            query_parts.append("WHERE " + " AND ".join(self._where_clauses))

        # ORDER BY clause
        if self._order_by:
            query_parts.append("ORDER BY " + ", ".join([_order_by.sql() for _order_by in self._order_by]))

        return " ".join(query_parts)
