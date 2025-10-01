import json
from typing import Any

def bashEncoder(obj: dict[str, Any]) -> str:
    content = []
    for k,v in obj.items():
        if (isinstance(v, list)):
            formatted_list = " ".join(list(v))
            content.append(f"{k}=({formatted_list})")
        elif (isinstance(v, dict)):
            raise ValueError("No implementation for nested dictionaries in bash encoder")
        else:
            content.append(f"{k}=\"{v}\"")
    return "\n".join(content)

def jsonEncoder(obj: dict[str, Any]) -> str:
    return json.dumps(obj)

