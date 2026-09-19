"""MCPServer (mcp SDK 2.x) registration for the write server."""
import functools

from prodtools_mcp_write import tools

# Computed without touching the MCP SDK, so registration coverage is
# checkable under the system python3.9 that runs test_unit.py (the
# real `mcp` package needs >=3.10). create_write_mcp_server() derives
# its registrations from this dict, so it and TOOL_NAMES cannot drift
# from what actually gets registered.
TOOL_FUNCTIONS = {
    'push_cnf': tools.push_cnf,
    'push_file': tools.push_file,
    'run_submissions': tools.run_submissions,
}

TOOL_NAMES = tuple(TOOL_FUNCTIONS)


def get_write_server_info():
    return {
        'name': 'prodtools-write',
        'performs_writes': True,
        'description': (
            'Write-capable prodtools submission. run_as="self" needs no '
            'privilege and writes only your own scratch, datasets and '
            'ledger. run_as="mu2epro" registers artifacts in production '
            'SAM and submits production grid jobs; it is refused unless '
            'confirm=true.'),
        'tools': list(TOOL_NAMES),
    }


def _forwarding(fn, ToolError):
    """mcp 2.x sends a ToolError's text to the client and hides every other
    exception behind "Error executing tool <name>". The tool functions
    refuse with ValueError/RuntimeError whose text IS the remedy (which
    file is missing, why mu2epro was refused, what the ledger holds), so
    re-raise each as a ToolError carrying the same text."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ToolError:
            raise
        except Exception as e:
            raise ToolError(str(e)) from e
    return wrapper


def create_write_mcp_server():
    from mcp.server.mcpserver import MCPServer
    from mcp.server.mcpserver.exceptions import ToolError

    mcp = MCPServer('prodtools-write')
    for name, fn in TOOL_FUNCTIONS.items():
        mcp.tool(name=name)(_forwarding(fn, ToolError))
    return mcp


def main():
    create_write_mcp_server().run()


if __name__ == '__main__':
    main()
