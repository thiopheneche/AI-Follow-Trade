"""Write service configuration without exposing secrets in command arguments."""

from pathlib import Path
import re
import tempfile


def write_private(path, content):
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as file:
        temporary = Path(file.name)
        try:
            file.write(content)
            file.close()
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)


def with_environment(original, values):
    keys = "|".join(re.escape(key) for key in values)
    lines = [line for line in (original or "").splitlines() if not re.match(rf"\s*(?:{keys})\s*=", line)]
    for key, value in values.items():
        quoted = re.sub(r'([\\"`$])', r'\\\1', value)
        lines.append(f'{key}="{quoted}"')
    return "\n".join([*lines, ""])
