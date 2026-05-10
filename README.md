# FOREMAN MCP Server

**Business Operations MCP Server** by CONFLUX SYSTEMS (PTY) LTD

## Endpoints

| Service | URL |
|---------|-----|
| HTTP API | `http://178.105.81.178:8084` |
| MCP Stream | `http://178.105.81.178:8084/mcp` |
| Health | `http://178.105.81.178:8084/health` |

## Tools

- **Clients**: add, get, update, delete
- **Invoices**: create, list, update status, generate PDF
- **Quotes**: create, list, convert to invoice
- **Payments**: record, list
- **Reports**: revenue summary, overdue invoices, client statements
- **Admin**: delete all data (confirm required)

## Features

- VAT calculation (15%)
- SQLite WAL mode for performance
- JWT authentication
- Rate limiting

## Version

1.1.0
